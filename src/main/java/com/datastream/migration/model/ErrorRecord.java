package com.datastream.migration.model;

import lombok.Data;
import java.util.Date;

/**
 * 异常数据记录
 * 用于追溯过滤去重的历史异常数据和入库异常数据
 * 
 * @author DataStream
 * @version 1.0
 */
@Data
public class ErrorRecord {
    
    /**
     * 主键 ID
     */
    private Long id;
    
    /**
     * 任务 ID
     */
    private String taskId;
    
    /**
     * 错误类型：DUPLICATE-重复数据，INSERT_ERROR-入库异常，QUERY_ERROR-查询异常
     */
    private String errorType;
    
    /**
     * 原始数据 ID
     */
    private Long originalDataId;
    
    /**
     * 去重关键字
     */
    private String duplicateKey;
    
    /**
     * 错误信息
     */
    private String errorMessage;
    
    /**
     * 堆栈跟踪
     */
    private String stackTrace;
    
    /**
     * 原始数据快照（JSON 格式）
     */
    private String dataSnapshot;
    
    /**
     * 发生时间
     */
    private Date occurTime;
    
    /**
     * 是否已处理
     */
    private Boolean processed;
    
    /**
     * 处理时间
     */
    private Date processTime;
    
    /**
     * 处理备注
     */
    private String processRemark;
}
