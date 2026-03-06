package com.datastream.migration.model;

import lombok.Builder;
import lombok.Data;
import java.util.Date;

/**
 * 迁移任务执行结果
 * 记录每次迁移任务的执行情况
 * 
 * @author DataStream
 * @version 1.0
 */
@Data
@Builder
public class MigrationResult {
    
    /**
     * 任务 ID
     */
    private String taskId;
    
    /**
     * 是否成功
     */
    private boolean success;
    
    /**
     * 迁移的总记录数
     */
    private long totalRecords;
    
    /**
     * 成功入库的记录数
     */
    private long successCount;
    
    /**
     * 过滤的重复记录数
     */
    private long filteredCount;
    
    /**
     * 失败的记录数
     */
    private long failedCount;
    
    /**
     * 开始时间
     */
    private Date startTime;
    
    /**
     * 结束时间
     */
    private Date endTime;
    
    /**
     * 耗时（毫秒）
     */
    private long durationMs;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 详细信息
     */
    private String detailInfo;
}
