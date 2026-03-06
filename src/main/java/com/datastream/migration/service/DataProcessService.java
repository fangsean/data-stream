package com.datastream.migration.service;

import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import java.util.List;

/**
 * 数据处理服务接口
 * 提供数据转换、验证、加工等功能
 * 
 * @author DataStream
 * @version 1.0
 */
public interface DataProcessService {
    
    /**
     * 初始化数据处理服务
     * 
     * @param config 迁移配置
     */
    void init(MigrationConfig config);
    
    /**
     * 处理单条数据
     * 包括数据转换、验证、清洗等
     * 
     * @param data 待处理的迁移数据
     * @return 处理后的迁移数据
     */
    MigrationData process(MigrationData data);
    
    /**
     * 批量处理数据
     * 
     * @param dataList 待处理的迁移数据列表
     * @return 处理后的迁移数据列表
     */
    List<MigrationData> processBatch(List<MigrationData> dataList);
    
    /**
     * 验证数据有效性
     * 
     * @param data 迁移数据
     * @return true-有效，false-无效
     */
    boolean validate(MigrationData data);
    
    /**
     * 数据转换
     * 将源数据格式转换为目标格式
     * 
     * @param data 源数据
     * @return 转换后的数据
     */
    MigrationData transform(MigrationData data);
    
    /**
     * 数据清洗
     * 去除无效、错误、冗余数据
     * 
     * @param data 待清洗的数据
     * @return 清洗后的数据
     */
    MigrationData clean(MigrationData data);
    
    /**
     * 保存数据到目标库
     * 
     * @param data 迁移数据
     * @return true-成功，false-失败
     */
    boolean saveToTarget(MigrationData data);
    
    /**
     * 批量保存数据到目标库
     * 
     * @param dataList 迁移数据列表
     * @return 成功的数量
     */
    int saveBatchToTarget(List<MigrationData> dataList);
    
    /**
     * 关闭服务
     */
    void close();
}
