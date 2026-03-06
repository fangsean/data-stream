package com.datastream.migration.filter;

import com.datastream.migration.model.MigrationData;
import java.util.List;

/**
 * 数据去重过滤器接口
 * 提供高效的大数据量去重功能，支持 BloomFilter 和磁盘存储
 * 
 * @author DataStream
 * @version 1.0
 */
public interface DuplicateFilter {
    
    /**
     * 初始化过滤器
     * 加载历史过滤数据到内存（如果存在）
     */
    void init();
    
    /**
     * 检查数据是否重复
     * 
     * @param data 待检查的迁移数据
     * @return true-重复，false-不重复
     */
    boolean isDuplicate(MigrationData data);
    
    /**
     * 批量检查数据是否重复
     * 
     * @param dataList 待检查的迁移数据列表
     * @return 过滤后的数据列表（不包含重复数据）
     */
    List<MigrationData> filterDuplicates(List<MigrationData> dataList);
    
    /**
     * 将数据添加到过滤器中
     * 
     * @param data 迁移数据
     */
    void add(MigrationData data);
    
    /**
     * 批量添加数据到过滤器中
     * 
     * @param dataList 迁移数据列表
     */
    void addAll(List<MigrationData> dataList);
    
    /**
     * 保存过滤器数据到磁盘
     * 用于持久化已过滤的数据集合
     */
    void saveToFile();
    
    /**
     * 从磁盘加载过滤器数据
     * 用于恢复之前保存的过滤数据集合
     */
    void loadFromFile();
    
    /**
     * 清空过滤器
     * 释放内存
     */
    void clear();
    
    /**
     * 获取过滤器中的元素数量
     * 
     * @return 元素数量
     */
    long size();
    
    /**
     * 销毁过滤器
     * 释放所有资源
     */
    void destroy();
}
