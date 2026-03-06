package com.datastream.migration.scheduler;

import com.datastream.migration.filter.DuplicateFilter;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import com.datastream.migration.model.MigrationResult;
import com.datastream.migration.service.DataProcessService;
import com.datastream.migration.service.DataSourceService;

import java.util.Date;
import java.util.List;

/**
 * 迁移任务执行器接口
 * 负责协调数据源、过滤器、处理器完成数据迁移
 * 
 * @author DataStream
 * @version 1.0
 */
public interface MigrationExecutor {
    
    /**
     * 初始化迁移执行器
     * 
     * @param config 迁移配置
     * @param dataSourceService 数据源服务
     * @param dataProcessService 数据处理服务
     * @param duplicateFilter 去重过滤器
     */
    void init(MigrationConfig config, DataSourceService dataSourceService, 
              DataProcessService dataProcessService, DuplicateFilter duplicateFilter);
    
    /**
     * 执行迁移任务
     * 
     * @return 迁移结果
     */
    MigrationResult execute();
    
    /**
     * 按日期分批执行迁移
     * 
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @return 迁移结果
     */
    MigrationResult executeByDateRange(Date startDate, Date endDate);
    
    /**
     * 暂停迁移任务
     */
    void pause();
    
    /**
     * 恢复迁移任务
     */
    void resume();
    
    /**
     * 停止迁移任务
     */
    void stop();
    
    /**
     * 获取当前进度（百分比）
     * 
     * @return 进度百分比
     */
    double getProgress();
    
    /**
     * 获取已处理的记录数
     * 
     * @return 已处理记录数
     */
    long getProcessedCount();
}
