package com.datastream.migration.strategy;

import com.datastream.migration.model.MigrationData;

/**
 * 去重策略接口
 */
public interface DuplicateStrategy {
    
    /**
     * 判断是否重复
     * 
     * @param key 去重键
     * @return true-重复，false-不重复
     */
    boolean isDuplicate(String key);
    
    /**
     * 添加重复数据（用于初始化已有数据）
     * 
     * @param key 去重键
     */
    void addDuplicate(String key);
    
    /**
     * 获取策略名称
     * 
     * @return 策略名称
     */
    String getStrategyName();
}
