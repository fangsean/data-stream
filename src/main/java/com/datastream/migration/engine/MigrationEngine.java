package com.datastream.migration.engine;

import com.datastream.migration.consumer.CheckpointConsumerWrapper;
import com.datastream.migration.consumer.DataConsumer;
import com.datastream.migration.enums.CheckpointType;
import com.datastream.migration.filter.DuplicateFilter;
import com.datastream.migration.handler.MigrationDataEvent;
import com.datastream.migration.handler.MigrationEventHandler;
import com.datastream.migration.producer.DataProducer;
import com.datastream.migration.task.ProducerTask;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据迁移引擎
 */
public class MigrationEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationEngine.class);
    
    private final DuplicateFilter duplicateFilter;
    private final DataConsumer consumer;
    private final int ringBufferSize;
    
    // 批量确认包装器（可选）
    private CheckpointConsumerWrapper checkpointConsumerWrapper;
    
    // 线程池
    private ExecutorService producerExecutor;
    
    private Disruptor<MigrationDataEvent> disruptor;
    private AtomicLong producedCount = new AtomicLong(0);
    private AtomicLong consumedCount = new AtomicLong(0);
    private AtomicLong duplicateCount = new AtomicLong(0);
    
    // ✅ 按天统计的 ConcurrentHashMap
    private final java.util.Map<String, AtomicLong> dailyProducedCounts = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, AtomicLong> dailyConsumedCounts = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, AtomicLong> dailyDuplicateCounts = new java.util.concurrent.ConcurrentHashMap<>();
    

    /**
     * 创建迁移引擎（支持批量确认）
     * 
     * @param duplicateFilter 去重过滤器
     * @param consumer 消费者
     * @param ringBufferSize Disruptor 环形缓冲区大小
     * @param enableCheckpoint 是否启用断点续传
     * @param checkpointThreshold 批量确认阈值（每处理多少条保存一次）
     * @param producerExecutor 生产者线程池（可选，为 null 时使用默认配置）
     */
    public MigrationEngine(DuplicateFilter duplicateFilter, 
                          DataConsumer consumer, 
                          int ringBufferSize,
                          boolean enableCheckpoint,
                          int checkpointThreshold,
                          ExecutorService producerExecutor) {
        this.duplicateFilter = duplicateFilter;
        this.ringBufferSize = ringBufferSize;
        this.producerExecutor = producerExecutor;

        // 如果启用断点续传，使用包装器
        if (enableCheckpoint) {
            this.checkpointConsumerWrapper = new CheckpointConsumerWrapper(
                consumer,
                CheckpointType.SOURCE_DATA_MIGRATE,  // A 库迁移场景
                "data/checkpoint",
                enableCheckpoint
            );
            this.consumer = this.checkpointConsumerWrapper;
            logger.info("✓ 批量确认已启用，阈值：{} 条", checkpointThreshold);
        } else {
            this.consumer = consumer;
            logger.info("✗ 批量确认未启用");
        }
    }
    
    /**
     * 启动引擎
     */
    public void start() {
        logger.info("========== 启动迁移引擎 ==========");
        
        // 初始化 Disruptor
        initDisruptor();
        
        logger.info("✓ 迁移引擎启动完成，RingBuffer 大小：{}", ringBufferSize);
    }
    
    /**
     * 执行迁移任务（使用线程池）
     */
    public void execute(List<DataProducer> producers) throws InterruptedException {
        logger.info("========== 开始执行迁移任务，生产者数量：{} ==========", producers.size());
        
        // 如果未提供线程池，创建临时线程池
        boolean needShutdown = false;
        if (producerExecutor == null) {
            logger.warn("⚠️ 未配置生产者线程池，使用临时线程池");
            producerExecutor = Executors.newFixedThreadPool(producers.size(), r -> {
                Thread thread = new Thread(r);
                thread.setName("Temp-Producer-Thread");
                thread.setDaemon(true);
                return thread;
            });
            needShutdown = true;
        }

        List<Future<?>> futures = new ArrayList<>();
        
        // 提交所有生产者任务到线程池
        for (int i = 0; i < producers.size(); i++) {
            String taskName = "Producer-" + i;
            ProducerTask task = new ProducerTask(producers.get(i), disruptor, taskName);
            Future<?> future = producerExecutor.submit(task);
            futures.add(future);
            logger.info("✓ {} 已提交到线程池", taskName);
        }
        
        // 等待所有生产者完成
        for (Future<?> future : futures) {
            try {
                future.get();  // 阻塞等待任务完成
            } catch (ExecutionException e) {
                logger.error("生产者任务执行失败", e.getCause());
            }
        }
        
        // 关闭临时线程池
        if (needShutdown) {
            producerExecutor.shutdown();
        }
        
        logger.info("✓ 所有生产者已完成（消费者继续处理中...）");
        logger.info("提示：统计数据由 MigrationEngineManager 每 5 秒输出一次");
        logger.info("======================================");
    }
    
    /**
     * 停止引擎
     */
    public void stop() {
        logger.info("========== 停止迁移引擎 ==========");
        
        if (disruptor != null) {
            disruptor.shutdown();
            logger.info("✓ Disruptor 已关闭");
        }
        
        if (consumer != null) {
            consumer.close();  // 会自动保存最终断点
        }
        
        logger.info("✓ 迁移引擎已停止");
        logger.info("最终统计 - 生产：{}, 消费：{}, 去重：{}", 
                producedCount.get(), consumedCount.get(), duplicateCount.get());
        logger.info("======================================");
    }
    
    /**
     * 初始化 Disruptor（使用线程池）
     */
    private void initDisruptor() {
        logger.info("正在初始化 Disruptor...");
        
        // 创建事件处理器
        MigrationEventHandler eventHandler = new MigrationEventHandler(consumer, consumedCount, duplicateCount);
        
        // 构建 Disruptor (4.0.0 API)
        // 使用 YieldWaitStrategy：性能更好，通过 yield CPU 来等待，延迟更低
        disruptor = new Disruptor<>(
            new MigrationDataEventFactory(),
            ringBufferSize,
            Executors.defaultThreadFactory(),
            ProducerType.MULTI,
//            new YieldingWaitStrategy()  //  使用 YieldWaitStrategy 替代 BlockingWaitStrategy
            new BusySpinWaitStrategy()
        );
        
        // 设置事件处理器
        disruptor.handleEventsWith(eventHandler);
        
        // 启动 Disruptor
        disruptor.start();
        
        logger.info("✓ Disruptor 初始化完成，RingBuffer 大小：{}, 等待策略：YieldWaitStrategy", ringBufferSize);
    }
    
    /**
     * 获取生产数量
     */
    public AtomicLong getProducedCount() {
        return producedCount;
    }
    
    /**
     * 获取消费数量
     */
    public AtomicLong getConsumedCount() {
        return consumedCount;
    }
    
    /**
     * 获取去重数量
     */
    public AtomicLong getDuplicateCount() {
        return duplicateCount;
    }
    
    /**
     * 获取去重过滤器
     */
    public DuplicateFilter getDuplicateFilter() {
        return duplicateFilter;
    }
    
    /**
     * ✅ 记录某一天的生产数量
     */
    public void recordDailyProduced(String dateStr, long count) {
        dailyProducedCounts.computeIfAbsent(dateStr, k -> new AtomicLong(0)).addAndGet(count);
        producedCount.addAndGet(count);
    }
    
    /**
     * ✅ 记录某一天的消费数量（入库 + 重复）
     */
    public void recordDailyConsumed(String dateStr, long insertCount, long duplicateCount) {
        dailyConsumedCounts.computeIfAbsent(dateStr, k -> new AtomicLong(0)).addAndGet(insertCount);
        dailyDuplicateCounts.computeIfAbsent(dateStr, k -> new AtomicLong(0)).addAndGet(duplicateCount);
        
        this.consumedCount.addAndGet(insertCount);
        this.duplicateCount.addAndGet(duplicateCount);
    }
    
    /**
     * ✅ 校验并保存某一天的断点（当该天数据处理完成后调用）
     * 
     * @param dateStr 日期字符串
     * @return true-验证通过，false-验证失败
     */
    public boolean checkAndSaveCheckpoint(String dateStr) {
        AtomicLong produced = dailyProducedCounts.get(dateStr);
        AtomicLong consumed = dailyConsumedCounts.get(dateStr);
        AtomicLong duplicates = dailyDuplicateCounts.get(dateStr);
        
        if (produced == null) produced.set(0L);
        if (consumed == null) consumed.set(0L);
        if (duplicates == null) duplicates.set(0L);
        
        // ✅ 应用校验公式：生产者天总数 = 消费入库数 + 判断重复数
        boolean isValid = (consumed.get() + duplicates.get() == produced.get());
        
        if (isValid) {
            // 验证通过，保存断点
            if (checkpointConsumerWrapper != null) {
                checkpointConsumerWrapper.markDateCompleted(dateStr, produced.get());
            }
            logger.info("✓ 日期 {} 验证通过：{} (生产) = {} (消费) + {} (重复)，已落盘", 
                       dateStr, produced, consumed, duplicates);
        } else {
            logger.error("✗ 日期 {} 验证失败！{} (生产) ≠ {} (消费) + {} (重复)", 
                        dateStr, produced, consumed, duplicates);
        }
        
        return isValid;
    }
    
    /**
     * 获取某一天的生产数量
     */
    public long getDailyProducedCount(String dateStr) {
        AtomicLong count = dailyProducedCounts.get(dateStr);
        return count != null ? count.get() : 0L;
    }
    
    /**
     * 获取某一天的消费数量
     */
    public long getDailyConsumedCount(String dateStr) {
        AtomicLong count = dailyConsumedCounts.get(dateStr);
        return count != null ? count.get() : 0L;
    }
    
    /**
     * 获取某一天的重复数量
     */
    public long getDailyDuplicateCount(String dateStr) {
        AtomicLong count = dailyDuplicateCounts.get(dateStr);
        return count != null ? count.get() : 0L;
    }
    
    /**
     * ✅ 获取所有已处理的日期列表
     */
    public java.util.Set<String> getProcessedDates() {
        // 返回所有有生产数据的日期
        return new java.util.HashSet<>(dailyProducedCounts.keySet());
    }
}
