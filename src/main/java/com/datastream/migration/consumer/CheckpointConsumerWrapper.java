package com.datastream.migration.consumer;

import com.datastream.migration.checkpoint.CheckpointManager;
import com.datastream.migration.enums.CheckpointType;
import com.datastream.migration.model.MigrationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 支持断点续传的消费者包装器
 *
 * 特性：
 * 1. ✅ 只有当某一天数据完全处理完成后，才标记该天为完成并落盘断点
 * 2. ✅ 服务异常中断后可以从断点恢复（不会丢失数据）
 * 3. ✅ 支持数量验证：生产者天总数 = 消费入库数 + 判断重复数
 *
 * 使用方式：
 * - 消费者处理数据时不保存断点
 * - 当某一天的所有数据处理完成后，调用 markDateCompleted() 方法标记完成
 * - 服务关闭时保存最终状态（用于异常宕机恢复）
 */
public class CheckpointConsumerWrapper implements DataConsumer {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointConsumerWrapper.class);

    /**
     * 实际的消费者
     */
    private final DataConsumer delegate;

    /**
     * 断点管理器
     */
    private final CheckpointManager checkpointManager;

    /**
     * 已处理数量计数器
     */
    private final AtomicLong processedCount = new AtomicLong(0);

    /**
     * 当前处理的日期
     */
    private volatile String currentDate;

    /**
     * 当前日期已入库的数量（成功插入的）
     */
    private final java.util.Map<String, Long> dateInsertCounts = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 当前日期重复的数量（被过滤的）
     */
    private final java.util.Map<String, Long> dateDuplicateCounts = new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * 日期格式化器
     */
    private static final SimpleDateFormat DATE_FORMATTER = new SimpleDateFormat("yyyy-MM-dd");

    public CheckpointConsumerWrapper(DataConsumer delegate,
                                     CheckpointType checkpointType,
                                     String checkpointDir,
                                     boolean enableCheckpoint) {
        this.delegate = delegate;
        this.checkpointManager = new CheckpointManager(checkpointDir, checkpointType, enableCheckpoint);

        // 加载断点
        if (enableCheckpoint) {
            this.checkpointManager.load();
            logger.info("✓ 断点续传已启用");
        } else {
            logger.info("✗ 断点续传未启用");
        }
    }

    @Override
    public void consume(List<MigrationData> dataList) {
        try {
            // 1. 委托给实际消费者处理（包含去重逻辑）
            delegate.consume(dataList);

            // 2. 更新总计数
            long count = processedCount.addAndGet(dataList.size());

            // 3. 提取当前日期并累计数量
            if (!dataList.isEmpty()) {
                java.util.Date businessDate = dataList.get(0).getBusinessDate();
                if (businessDate != null) {
                    this.currentDate = DATE_FORMATTER.format(businessDate);

                    // ⚠️ 注意：这里累计的是总处理数量（包括入库和重复的）
                    // 实际的入库数和重复数需要由外部提供
                    dateInsertCounts.merge(this.currentDate, (long) dataList.size(), Long::sum);
                }
            }

            logger.trace("【消费者】本轮消费 {} 条，累计：{} 条", dataList.size(), count);

        } catch (Exception e) {
            logger.error("消费数据失败", e);
            throw e;  // 抛出异常让 Disruptor 重试
        }
    }

    /**
     * 标记某个日期为已完成（带完整统计验证）
     * 只有当消费者成功处理完一整天的数据后才调用此方法
     *
     * @param date 日期
     * @param expectedTotal 预期总数（生产者生成的数量）
     * @return true-验证通过，false-验证失败
     */
    public boolean markDateCompleted(String date, long expectedTotal) {
        // 从统计 Map 中获取实际入库数和重复数
        Long actualInsertCount = dateInsertCounts.get(date);
        Long duplicateCount = dateDuplicateCounts.get(date);

        if (actualInsertCount == null) {
            actualInsertCount = 0L;
        }
        if (duplicateCount == null) {
            duplicateCount = 0L;
        }

        // ✅ 使用校验公式：生产者天总数 = 消费入库数 + 判断重复数
        boolean isValid = (actualInsertCount + duplicateCount == expectedTotal);

        if (isValid) {
            // ✅ 只有验证通过才保存断点
            checkpointManager.markCompleted(date, actualInsertCount, duplicateCount, expectedTotal);
            checkpointManager.save();
            logger.info("✓ 日期 {} 验证通过：{} (生产) = {} (入库) + {} (重复)，已落盘断点",
                       date, expectedTotal, actualInsertCount, duplicateCount);
        } else {
            logger.error("✗ 日期 {} 验证失败！{} (生产) ≠ {} (入库) + {} (重复)，不保存断点",
                        date, expectedTotal, actualInsertCount, duplicateCount);
        }

        return isValid;
    }

    /**
     * 保存断点
     */
    private void saveCheckpoint(String date) {
        if (date != null) {
            checkpointManager.markCompleted(date);
            checkpointManager.save();
        }
    }

    /**
     * 检查日期是否已完成
     */
    public boolean isCompleted(String date) {
        return checkpointManager.isCompleted(date);
    }

    /**
     * 获取已处理数量
     */
    public long getProcessedCount() {
        return processedCount.get();
    }

    /**
     * 关闭时保存最终断点
     */
    @Override
    public void close() {
        logger.info("正在关闭消费者，保存最终断点...");

        // 保存最终状态
        checkpointManager.save();
        
        // 关闭实际消费者
        delegate.close();
        
        logger.info("✓ 消费者已关闭，最终处理 {} 条数据", processedCount.get());
    }
}
