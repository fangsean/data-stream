package com.datastream.migration.engine;

import com.datastream.migration.consumer.DataConsumer;
import com.datastream.migration.filter.DuplicateFilter;
import com.datastream.migration.handler.MigrationDataEvent;
import com.datastream.migration.handler.MigrationEventHandler;
import com.datastream.migration.producer.DataProducer;
import com.datastream.migration.task.ProducerTask;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 数据迁移引擎
 */
public class MigrationEngine {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationEngine.class);
    
    private final DuplicateFilter duplicateFilter;
    private final DataConsumer consumer;
    private final int ringBufferSize;
    
    private Disruptor<MigrationDataEvent> disruptor;
    private AtomicLong producedCount = new AtomicLong(0);
    private AtomicLong consumedCount = new AtomicLong(0);
    private AtomicLong duplicateCount = new AtomicLong(0);
    
    public MigrationEngine(DuplicateFilter duplicateFilter, 
                          DataConsumer consumer, 
                          int ringBufferSize) {
        this.duplicateFilter = duplicateFilter;
        this.consumer = consumer;
        this.ringBufferSize = ringBufferSize;
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
     * 执行迁移任务
     */
    public void execute(List<DataProducer> producers) throws InterruptedException {
        logger.info("========== 开始执行迁移任务，生产者数量：{} ==========", producers.size());
        
        // 创建并启动所有生产者线程
        List<Thread> producerThreads = new ArrayList<>();
        for (int i = 0; i < producers.size(); i++) {
            String taskName = "Producer-" + i;
            ProducerTask task = new ProducerTask(producers.get(i), disruptor, taskName);
            Thread thread = new Thread(task, taskName);
            producerThreads.add(thread);
            thread.start();
            logger.info("✓ {} 已启动", taskName);
        }
        
        // 等待所有生产者完成
        for (Thread thread : producerThreads) {
            thread.join();
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
            consumer.close();
        }
        
        logger.info("✓ 迁移引擎已停止");
        logger.info("最终统计 - 生产：{}, 消费：{}, 去重：{}", 
                producedCount.get(), consumedCount.get(), duplicateCount.get());
        logger.info("======================================");
    }
    
    /**
     * 初始化 Disruptor
     */
    private void initDisruptor() {
        logger.info("正在初始化 Disruptor...");
        
        // 创建事件处理器
        MigrationEventHandler eventHandler = new MigrationEventHandler(consumer, consumedCount, duplicateCount);
        
        // 构建 Disruptor (4.0.0 API)
        disruptor = new Disruptor<>(
            new MigrationDataEventFactory(),
            ringBufferSize,
            new ThreadFactory() {
                @Override
                public Thread newThread(Runnable r) {
                    return new Thread(r, "Disruptor-Consumer-Thread");
                }
            },
            ProducerType.MULTI,
            new BlockingWaitStrategy()
        );
        
        // 设置事件处理器
        disruptor.handleEventsWith(eventHandler);
        
        // 启动 Disruptor
        disruptor.start();
        
        logger.info("✓ Disruptor 初始化完成");
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
}
