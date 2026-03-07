package com.datastream.migration.strategy;

/**
 * 数据加载策略接口
 */
public interface DataLoadStrategy {
    
    /**
     * 执行数据加载
     * 
     * @param startDate 开始日期（yyyy-MM-dd）
     * @param endDate 结束日期（yyyy-MM-dd）
     */
    void loadData(String startDate, String endDate);
    
    /**
     * 获取加载器名称
     * 
     * @return 加载器名称
     */
    String getName();
}
