package com.datastream.migration.consumer;

import com.datastream.migration.model.MigrationData;

import java.util.List;

/**
 * 消费者接口
 */
public interface DataConsumer {
    
    /**
     * 消费数据
     * @param dataList 数据列表
     */
    void consume(List<MigrationData> dataList);
    
    /**
     * 关闭资源
     */
    void close();
}
