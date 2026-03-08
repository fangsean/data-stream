package com.datastream.migration.handler;

import com.datastream.migration.consumer.DataConsumer;
import com.datastream.migration.model.MigrationData;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Disruptor 事件处理器
 */
public class MigrationEventHandler implements EventHandler<MigrationDataEvent> {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationEventHandler.class);
    
    // 去重日志（输出到 duplicate-check.log）
    private static final Logger duplicateLogger = LoggerFactory.getLogger("com.datastream.migration.duplicate");
    
    private final DataConsumer consumer;
    private final AtomicLong consumedCount;
    private final AtomicLong duplicateCount;
    
    public MigrationEventHandler(DataConsumer consumer, AtomicLong consumedCount, AtomicLong duplicateCount) {
        this.consumer = consumer;
        this.consumedCount = consumedCount;
        this.duplicateCount = duplicateCount;
    }
    
    @Override
    public void onEvent(MigrationDataEvent event, long sequence, boolean endOfBatch) throws Exception {
        try {
            List<MigrationData> dataList = event.getDataList();
            if (dataList != null && !dataList.isEmpty()) {
                logger.debug("【Disruptor】处理事件，序列号：{}, 数据量：{}", sequence, dataList.size());
                
                // 记录去重日志
                logDuplicateCheck(dataList);
                
                // ⚠️ 注意：不在这里计数，因为异常会导致 Disruptor 重新投递
                // 由消费者内部统计实际处理的数量
                consumer.consume(dataList);
                
                logger.trace("【Disruptor】消费完成，序列号：{}", sequence);
            }
        } catch (Exception e) {
            logger.error("【Disruptor】处理事件失败，序列号：{}", sequence, e);
            // ⚠️ 不抛出异常，避免 Disruptor 重新投递导致重复计数
            // 但是记录错误日志
        }
    }
    
    /**
     * 记录去重检查日志
     */
    private void logDuplicateCheck(List<MigrationData> dataList) {
        for (MigrationData data : dataList) {
            if (data.getDuplicateKey() != null && !data.getDuplicateKey().trim().isEmpty()) {
                duplicateLogger.info("DUPLICATE_CHECK|user_code={}|id={}|business_time={}",
                        data.getDuplicateKey(),
                        data.getId(),
                        data.getBusinessDate());
                duplicateCount.incrementAndGet();
            }
        }
    }
}
