package com.datastream.migration.model;

import lombok.Builder;
import lombok.Data;
import java.util.Date;

/**
 * 迁移任务配置
 * 定义迁移任务的参数和规则
 * 
 * @author DataStream
 * @version 1.0
 */
@Data
@Builder
public class MigrationConfig {
    
    /**
     * 任务 ID
     */
    private String taskId;
    
    /**
     * 任务名称
     */
    private String taskName;
    
    /**
     * 源数据库连接
     */
    private String sourceDbConnection;
    
    /**
     * 目标数据库连接
     */
    private String targetDbConnection;
    
    /**
     * 源表名
     */
    private String sourceTable;
    
    /**
     * 目标表名
     */
    private String targetTable;
    
    /**
     * 去重字段列表（逗号分隔）
     */
    private String duplicateFields;
    
    /**
     * 每批次查询的数据量
     */
    private int batchSize = 10000;
    
    /**
     * 开始日期（包含）
     */
    private Date startDate;
    
    /**
     * 结束日期（包含）
     */
    private Date endDate;
    
    /**
     * 并发线程数
     */
    private int threadCount = 4;
    
    /**
     * 是否启用 BloomFilter
     */
    private boolean enableBloomFilter = true;
    
    /**
     * BloomFilter 预期数据量（用于计算误判率）
     */
    private long expectedInsertions = 1000000000L;
    
    /**
     * BloomFilter 误判率（默认 0.01%）
     */
    private double fpp = 0.0001;
    
    /**
     * 过滤数据存储路径
     */
    private String filterDataPath;
    
    /**
     * 异常数据存储路径
     */
    private String errorDataPath;
    
    /**
     * 是否记录详细日志
     */
    private boolean detailedLogging = true;
}
