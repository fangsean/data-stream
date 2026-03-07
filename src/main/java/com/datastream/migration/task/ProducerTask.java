package com.datastream.migration.task;

import com.datastream.migration.model.MigrationData;
import com.datastream.migration.producer.DataProducer;
import com.datastream.migration.handler.MigrationDataEvent;
import com.lmax.disruptor.dsl.Disruptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * 生产者任务
 */
public class ProducerTask implements Runnable {
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerTask.class);
    
    private final DataProducer producer;
    private final Disruptor<MigrationDataEvent> disruptor;
    private final String taskName;
    
    public ProducerTask(DataProducer producer, 
                       Disruptor<MigrationDataEvent> disruptor, 
                       String taskName) {
        this.producer = producer;
        this.disruptor = disruptor;
        this.taskName = taskName;
    }
    
    @Override
    public void run() {
        try {
            logger.info("========== 【{}】开始工作 ==========", taskName);
            
            int batchCount = 0;
            long totalProduced = 0;
            
            while (producer.hasMore()) {
                // 生产数据
                List<MigrationData> dataList = producer.produce();

                if (dataList == null || dataList.isEmpty()) {
                    break;
                }
                
                batchCount++;
                
                // 发布到 Disruptor
                for (MigrationData data : dataList) {
                    disruptor.publishEvent((event, sequence) -> {
                        event.setDataList(java.util.Collections.singletonList(data));
                    });
                }
                
                totalProduced += dataList.size();
                logger.info("【{}】批次 {}，已发布 {} 条数据到 Disruptor，累计：{}", 
                        taskName, batchCount, dataList.size(), totalProduced);
            }
            
            logger.info("========== 【{}】完成，共处理 {} 批次，产出 {} 条 ==========", 
                    taskName, batchCount, totalProduced);
            
        } catch (Exception e) {
            logger.error("【{}】异常终止", taskName, e);
        } finally {
            producer.close();
        }
    }
}
