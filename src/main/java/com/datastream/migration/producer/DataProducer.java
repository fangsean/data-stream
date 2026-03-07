package com.datastream.migration.producer;

import com.datastream.migration.model.MigrationData;

import java.util.List;

/**
 * 生产者接口
 */
public interface DataProducer {
    
    /**
     * 生产数据
     * @return 数据列表
     */
    List<MigrationData> produce();
    
    /**
     * 是否还有更多数据
     * @return true/false
     */
    boolean hasMore();
    
    /**
     * 关闭资源
     */
    void close();
}
