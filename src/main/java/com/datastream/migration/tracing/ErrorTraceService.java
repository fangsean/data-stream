package com.datastream.migration.tracing;

import com.datastream.migration.model.ErrorRecord;
import java.util.Date;
import java.util.List;

/**
 * 错误追溯服务接口
 * 提供异常数据的查询、统计、导出等功能
 * 
 * @author DataStream
 * @version 1.0
 */
public interface ErrorTraceService {
    
    /**
     * 初始化错误追溯服务
     */
    void init();
    
    /**
     * 记录错误
     * 
     * @param errorRecord 错误记录
     */
    void recordError(ErrorRecord errorRecord);
    
    /**
     * 根据任务 ID 查询错误记录
     * 
     * @param taskId 任务 ID
     * @return 错误记录列表
     */
    List<ErrorRecord> queryByTaskId(String taskId);
    
    /**
     * 根据错误类型查询错误记录
     * 
     * @param errorType 错误类型
     * @return 错误记录列表
     */
    List<ErrorRecord> queryByErrorType(String errorType);
    
    /**
     * 根据时间范围查询错误记录
     * 
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 错误记录列表
     */
    List<ErrorRecord> queryByTimeRange(Date startTime, Date endTime);
    
    /**
     * 获取指定任务的错误统计
     * 
     * @param taskId 任务 ID
     * @return 错误统计信息（JSON 格式）
     */
    String getErrorStatistics(String taskId);
    
    /**
     * 标记错误记录为已处理
     * 
     * @param errorId 错误记录 ID
     * @param remark 处理备注
     */
    void markAsProcessed(Long errorId, String remark);
    
    /**
     * 批量标记错误记录为已处理
     * 
     * @param errorIds 错误记录 ID 列表
     * @param remark 处理备注
     */
    void batchMarkAsProcessed(List<Long> errorIds, String remark);
    
    /**
     * 导出错误记录到文件
     * 
     * @param taskId 任务 ID
     * @param filePath 导出文件路径
     */
    void exportToFile(String taskId, String filePath);
    
    /**
     * 清空错误记录
     * 
     * @param taskId 任务 ID
     */
    void clearErrors(String taskId);
    
    /**
     * 关闭服务
     */
    void close();
}
