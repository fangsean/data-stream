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
     *  原子性的 "检查并添加" 操作（可选实现）
     * 如果 key 不存在，则添加并返回 false（不重复）
     * 如果 key 已存在，则返回 true（重复）
     * 
     * @param key 要检查和添加的 key
     * @return true-重复（已存在），false-不重复（新添加）
     */
    default boolean checkAndAdd(String key) {
        // 默认实现：先检查后添加（非原子）
        if (isDuplicate(key)) {
            return true;
        } else {
            addDuplicate(key);
            return false;
        }
    }
    
    /**
     * 获取策略名称
     * 
     * @return 策略名称
     */
    String getStrategyName();
}
