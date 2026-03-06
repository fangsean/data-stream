package com.datastream.migration.tracing.impl;

import com.datastream.migration.model.ErrorRecord;
import com.datastream.migration.tracing.ErrorTraceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 错误追溯服务实现类
 * 提供错误记录的存储、查询、统计、导出等功能
 * 
 * @author DataStream
 * @version 1.0
 */
public class ErrorTraceServiceImpl implements ErrorTraceService {
    
    private static final Logger logger = LoggerFactory.getLogger(ErrorTraceServiceImpl.class);
    
    /**
     * 错误记录存储 - 使用 ConcurrentHashMap 保证线程安全
     * Key: errorId, Value: ErrorRecord
     */
    private final ConcurrentHashMap<Long, ErrorRecord> errorRecordMap;
    
    /**
     * 任务 ID 索引 - 用于快速查询
     * Key: taskId, Value: errorId 列表
     */
    private final ConcurrentHashMap<String, Set<Long>> taskIdIndex;
    
    /**
     * 错误类型索引
     * Key: errorType, Value: errorId 列表
     */
    private final ConcurrentHashMap<String, Set<Long>> errorTypeIndex;
    
    /**
     * 错误数据存储目录
     */
    private final String errorDataDir;
    
    /**
     * 构造函数
     */
    public ErrorTraceServiceImpl() {
        this.errorRecordMap = new ConcurrentHashMap<>();
        this.taskIdIndex = new ConcurrentHashMap<>();
        this.errorTypeIndex = new ConcurrentHashMap<>();
        this.errorDataDir = "error_data";
        
        // 创建错误数据目录
        File dir = new File(errorDataDir);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }
    
    /**
     * 初始化错误追溯服务
     */
    @Override
    public void init() {
        logger.info("错误追溯服务初始化完成");
    }
    
    /**
     * 记录错误
     * 线程安全
     * 
     * @param errorRecord 错误记录
     */
    @Override
    public void recordError(ErrorRecord errorRecord) {
        if (errorRecord == null || errorRecord.getId() == null) {
            logger.warn("错误记录为空或 ID 为空，跳过记录");
            return;
        }
        
        try {
            // 添加到主存储
            errorRecordMap.put(errorRecord.getId(), errorRecord);
            
            // 更新任务 ID 索引
            updateTaskIdIndex(errorRecord);
            
            // 更新错误类型索引
            updateErrorTypeIndex(errorRecord);
            
            logger.debug("错误记录已保存，ID: {}, 类型：{}", 
                    errorRecord.getId(), errorRecord.getErrorType());
        } catch (Exception e) {
            logger.error("记录错误失败", e);
        }
    }
    
    /**
     * 根据任务 ID 查询错误记录
     * 
     * @param taskId 任务 ID
     * @return 错误记录列表
     */
    @Override
    public List<ErrorRecord> queryByTaskId(String taskId) {
        if (taskId == null || taskId.trim().isEmpty()) {
            return new ArrayList<>();
        }
        
        List<ErrorRecord> resultList = new ArrayList<>();
        Set<Long> errorIds = taskIdIndex.get(taskId);
        
        if (errorIds != null && !errorIds.isEmpty()) {
            for (Long errorId : errorIds) {
                ErrorRecord record = errorRecordMap.get(errorId);
                if (record != null) {
                    resultList.add(record);
                }
            }
        }
        
        // 按发生时间排序
        Collections.sort(resultList, new Comparator<ErrorRecord>() {
            @Override
            public int compare(ErrorRecord o1, ErrorRecord o2) {
                return o2.getOccurTime().compareTo(o1.getOccurTime());
            }
        });
        
        logger.info("查询任务 {} 的错误记录，数量：{}", taskId, resultList.size());
        return resultList;
    }
    
    /**
     * 根据错误类型查询错误记录
     * 
     * @param errorType 错误类型
     * @return 错误记录列表
     */
    @Override
    public List<ErrorRecord> queryByErrorType(String errorType) {
        if (errorType == null || errorType.trim().isEmpty()) {
            return new ArrayList<>();
        }
        
        List<ErrorRecord> resultList = new ArrayList<>();
        Set<Long> errorIds = errorTypeIndex.get(errorType);
        
        if (errorIds != null && !errorIds.isEmpty()) {
            for (Long errorId : errorIds) {
                ErrorRecord record = errorRecordMap.get(errorId);
                if (record != null) {
                    resultList.add(record);
                }
            }
        }
        
        logger.info("查询错误类型 {} 的记录，数量：{}", errorType, resultList.size());
        return resultList;
    }
    
    /**
     * 根据时间范围查询错误记录
     * 
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 错误记录列表
     */
    @Override
    public List<ErrorRecord> queryByTimeRange(Date startTime, Date endTime) {
        if (startTime == null || endTime == null) {
            return new ArrayList<>();
        }
        
        List<ErrorRecord> resultList = new ArrayList<>();
        
        for (ErrorRecord record : errorRecordMap.values()) {
            if (record.getOccurTime() != null &&
                !record.getOccurTime().before(startTime) &&
                !record.getOccurTime().after(endTime)) {
                resultList.add(record);
            }
        }
        
        // 按发生时间排序
        Collections.sort(resultList, new Comparator<ErrorRecord>() {
            @Override
            public int compare(ErrorRecord o1, ErrorRecord o2) {
                return o2.getOccurTime().compareTo(o1.getOccurTime());
            }
        });
        
        logger.info("查询时间范围 {} 到 {} 的错误记录，数量：{}", startTime, endTime, resultList.size());
        return resultList;
    }
    
    /**
     * 获取指定任务的错误统计
     * 
     * @param taskId 任务 ID
     * @return 错误统计信息（JSON 格式）
     */
    @Override
    public String getErrorStatistics(String taskId) {
        List<ErrorRecord> errorRecords = queryByTaskId(taskId);
        
        Map<String, Object> statistics = new HashMap<>();
        statistics.put("taskId", taskId);
        statistics.put("totalErrors", errorRecords.size());
        
        // 按错误类型统计
        Map<String, Integer> typeCount = new HashMap<>();
        for (ErrorRecord record : errorRecords) {
            String errorType = record.getErrorType();
            typeCount.put(errorType, typeCount.getOrDefault(errorType, 0) + 1);
        }
        statistics.put("errorsByType", typeCount);
        
        // 按处理状态统计
        long processedCount = 0;
        long unprocessedCount = 0;
        for (ErrorRecord record : errorRecords) {
            if (record.getProcessed() != null && record.getProcessed()) {
                processedCount++;
            } else {
                unprocessedCount++;
            }
        }
        statistics.put("processedCount", processedCount);
        statistics.put("unprocessedCount", unprocessedCount);
        
        // 转换为简单的 JSON 字符串
        StringBuilder jsonBuilder = new StringBuilder("{");
        jsonBuilder.append("\"taskId\":\"").append(taskId).append("\",");
        jsonBuilder.append("\"totalErrors\":").append(errorRecords.size()).append(",");
        jsonBuilder.append("\"processedCount\":").append(processedCount).append(",");
        jsonBuilder.append("\"unprocessedCount\":").append(unprocessedCount);
        jsonBuilder.append("}");
        
        logger.info("获取任务 {} 的错误统计：总数={}, 已处理={}, 未处理={}", 
                taskId, errorRecords.size(), processedCount, unprocessedCount);
        
        return jsonBuilder.toString();
    }
    
    /**
     * 标记错误记录为已处理
     * 
     * @param errorId 错误记录 ID
     * @param remark 处理备注
     */
    @Override
    public void markAsProcessed(Long errorId, String remark) {
        ErrorRecord record = errorRecordMap.get(errorId);
        if (record != null) {
            record.setProcessed(true);
            record.setProcessTime(new Date());
            record.setProcessRemark(remark);
            
            logger.info("错误记录已标记为已处理，ID: {}, 备注：{}", errorId, remark);
        } else {
            logger.warn("错误记录不存在，ID: {}", errorId);
        }
    }
    
    /**
     * 批量标记错误记录为已处理
     * 
     * @param errorIds 错误记录 ID 列表
     * @param remark 处理备注
     */
    @Override
    public void batchMarkAsProcessed(List<Long> errorIds, String remark) {
        if (errorIds == null || errorIds.isEmpty()) {
            return;
        }
        
        int successCount = 0;
        for (Long errorId : errorIds) {
            ErrorRecord record = errorRecordMap.get(errorId);
            if (record != null) {
                record.setProcessed(true);
                record.setProcessTime(new Date());
                record.setProcessRemark(remark);
                successCount++;
            }
        }
        
        logger.info("批量标记错误记录完成，总数：{}, 成功：{}", errorIds.size(), successCount);
    }
    
    /**
     * 导出错误记录到文件
     * 
     * @param taskId 任务 ID
     * @param filePath 导出文件路径
     */
    @Override
    public void exportToFile(String taskId, String filePath) {
        List<ErrorRecord> errorRecords = queryByTaskId(taskId);
        
        if (errorRecords.isEmpty()) {
            logger.warn("没有可导出的错误记录，task: {}", taskId);
            return;
        }
        
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        
        try {
            fos = new FileOutputStream(filePath);
            oos = new ObjectOutputStream(fos);
            
            // 写入记录数量
            oos.writeInt(errorRecords.size());
            
            // 逐条写入
            for (ErrorRecord record : errorRecords) {
                oos.writeObject(record);
            }
            
            logger.info("错误记录导出成功，文件：{}, 数量：{}", filePath, errorRecords.size());
        } catch (IOException e) {
            logger.error("导出错误记录失败", e);
            throw new RuntimeException("导出错误记录失败", e);
        } finally {
            closeResource(oos, fos);
        }
    }
    
    /**
     * 清空错误记录
     * 
     * @param taskId 任务 ID
     */
    @Override
    public void clearErrors(String taskId) {
        if (taskId == null || taskId.trim().isEmpty()) {
            return;
        }
        
        Set<Long> errorIds = taskIdIndex.remove(taskId);
        if (errorIds != null && !errorIds.isEmpty()) {
            for (Long errorId : errorIds) {
                errorRecordMap.remove(errorId);
            }
            
            logger.info("清空任务 {} 的错误记录，数量：{}", taskId, errorIds.size());
        } else {
            logger.info("任务 {} 没有错误记录可清空", taskId);
        }
    }
    
    /**
     * 关闭服务
     */
    @Override
    public void close() {
        try {
            // 可以在这里将内存中的错误记录持久化到磁盘
            logger.info("错误追溯服务已关闭");
        } catch (Exception e) {
            logger.warn("关闭错误追溯服务失败", e);
        }
    }
    
    /**
     * 更新任务 ID 索引
     * 
     * @param record 错误记录
     */
    private void updateTaskIdIndex(ErrorRecord record) {
        String taskId = record.getTaskId();
        if (taskId != null && !taskId.trim().isEmpty()) {
            Set<Long> errorIds = taskIdIndex.computeIfAbsent(taskId, k -> ConcurrentHashMap.newKeySet());
            errorIds.add(record.getId());
        }
    }
    
    /**
     * 更新错误类型索引
     * 
     * @param record 错误记录
     */
    private void updateErrorTypeIndex(ErrorRecord record) {
        String errorType = record.getErrorType();
        if (errorType != null && !errorType.trim().isEmpty()) {
            Set<Long> errorIds = errorTypeIndex.computeIfAbsent(errorType, k -> ConcurrentHashMap.newKeySet());
            errorIds.add(record.getId());
        }
    }
    
    /**
     * 关闭资源
     * 
     * @param closeables 可关闭的资源
     */
    private void closeResource(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    logger.warn("关闭资源失败", e);
                }
            }
        }
    }
}
