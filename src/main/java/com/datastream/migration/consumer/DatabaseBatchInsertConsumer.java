package com.datastream.migration.consumer;

import com.datastream.migration.filter.RocksDBDuplicateFilter;
import com.datastream.migration.model.MigrationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

/**
 * 数据库批量插入消费者
 */
public class DatabaseBatchInsertConsumer implements DataConsumer {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseBatchInsertConsumer.class);
    
    // 单独的入库异常日志（输出到 db-insert-error.log）
    private static final Logger dbErrorLogger = LoggerFactory.getLogger("com.datastream.migration.db.error");
    
    // 去重日志（输出到 duplicate-check.log）
    private static final Logger duplicateLogger = LoggerFactory.getLogger("com.datastream.migration.duplicate");
    
    private final JdbcTemplate targetJdbc;
    private final String insertSql;
    private final String insertErrorSql;  // 错误表插入 SQL
    private final int batchSize;
    private RocksDBDuplicateFilter rocksDBFilter;  // 用于清理异常的 key
    private long totalConsumed = 0;
    private long batchCount = 0;
    private long errorCount = 0;
    private long duplicateCount = 0;  // 去重计数
    
    // ✅ 最近批次的统计信息（用于监控器）
    private volatile long lastBatchInsertCount = 0;
    private volatile long lastBatchDuplicateCount = 0;
    
    public DatabaseBatchInsertConsumer(JdbcTemplate targetJdbc, 
                                        String insertSql, 
                                        String insertErrorSql,
                                        int batchSize) {
        this.targetJdbc = targetJdbc;
        this.insertSql = insertSql;
        this.insertErrorSql = insertErrorSql;
        this.batchSize = batchSize;
    }
    
    /**
     * 设置 RocksDB 过滤器（用于异常时清理 key）
     */
    public void setRocksDBFilter(RocksDBDuplicateFilter rocksDBFilter) {
        this.rocksDBFilter = rocksDBFilter;
    }
    
    @Override
    public void consume(List<MigrationData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        
        try {
            logger.debug("【消费者】收到 {} 条数据", dataList.size());
            
            // 分批插入
            for (int i = 0; i < dataList.size(); i += batchSize) {
                int start = i;
                int end = Math.min(i + batchSize, dataList.size());
                List<MigrationData> batch = dataList.subList(start, end);
                
                try {
                    executeBatch(batch);
                    batchCount++;
                    
                    logger.debug("【消费者】批次 {} 插入完成，范围：{}-{}", 
                            batchCount, start, end);
                    
                } catch (org.springframework.dao.DuplicateKeyException e) {
                    // ⚠️ 主键冲突：这是正常的重复数据，不是错误
                    // 记录但不抛出，继续处理后续数据
                    logger.warn("⚠ 批次 {} 出现主键冲突（正常重复），范围：{}-{}，跳过该批次剩余数据",
                            batchCount, start, end, e);
                    
                    // 逐条处理该批次，统计重复数量
                    handleBatchWithDuplicates(batch, start);
                    
                } catch (Exception e) {
                    // 其他数据库异常（连接失败、死锁等）
                    logger.error("✗ 批次 {} 插入失败，范围：{}-{}", batchCount, start, end, e);
                    throw new RuntimeException("数据库插入失败", e);
                }
            }
            
            //  只有全部批次都成功处理后才更新计数
            totalConsumed += dataList.size();
            
            // ✅ 更新最近批次的统计信息
            lastBatchInsertCount = dataList.size() - duplicateCount;
            lastBatchDuplicateCount = duplicateCount;
                
        } catch (Exception e) {
            logger.error("【消费者】消费失败，总数据量：{}", dataList.size(), e);
                
            // 记录失败的数据详情到单独的日志文件
            logFailedData(dataList, e);
                
            // ⚠️ 注意：这里不抛出异常，避免 Disruptor 重新投递导致重复计数
            // 但是已经失败的数据不会计入 totalConsumed
            lastBatchInsertCount = 0;
            lastBatchDuplicateCount = 0;
        }
    }
    
    /**
     * 执行单批次插入
     */
    private void executeBatch(List<MigrationData> batch) {
        targetJdbc.batchUpdate(insertSql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
            @Override
            public void setValues(java.sql.PreparedStatement ps, int j) throws java.sql.SQLException {
                MigrationData data = batch.get(j);
                try {
                    setPreparedStatement(ps, data, j);
                } catch (Exception e) {
                    // 记录单条数据异常到单独日志
                    logSingleRecordError(data, e);
                    errorCount++;
                    throw e;  // 重新抛出，让上层知道有错误
                }
            }
            
            @Override
            public int getBatchSize() {
                return batch.size();
            }
        });
    }
    
    /**
     * 处理包含重复数据的批次（逐条处理，统计重复数量）
     */
    private void handleBatchWithDuplicates(List<MigrationData> batch, int startIndex) {
        logger.debug("正在逐条处理批次，起始索引：{}", startIndex);
        
        int batchDuplicateCount = 0;  // 本批次重复数据计数（局部变量）
        
        for (int i = 0; i < batch.size(); i++) {
            MigrationData data = batch.get(i);
            try {
                // ⚠️ 注意：这里不能再执行 insertSql，因为已经确定是重复数据了

                logger.warn("⊘ 重复数据跳过：id={}, user_code={}", data.getId(), data.getDuplicateKey());
                batchDuplicateCount++;

                // ✅ 清理 RocksDB 中的 key（必须执行，否则会导致二次校验失败）
                if (rocksDBFilter != null && data.getDuplicateKey() != null) {
                    try {
                        rocksDBFilter.removeDuplicate(data.getDuplicateKey());
                        logger.trace("✓ 已清理 RocksDB 中的重复 key: {}", data.getDuplicateKey());
                    } catch (Exception cleanupEx) {
                        logger.error("✗ 清理 RocksDB key 失败：{}", data.getDuplicateKey(), cleanupEx);
                    }
                }

            } catch (Exception e) {
                // 其他异常
                logger.error("✗ 处理重复数据失败：id={}, user_code={}", data.getId(), data.getDuplicateKey(), e);
                // logSingleRecordError(data, e);
            }
        }
        
        // ✅ 累加到成员变量（用于统计）
        this.duplicateCount += batchDuplicateCount;
        
        //  只在批次结束时输出一次统计
        if (batchDuplicateCount > 0) {
            logger.info("批次处理完成，起始索引：{}, 重复数据：{} 条", startIndex, batchDuplicateCount);
        }
        
        // ✅ 更新最近批次的统计信息
        lastBatchInsertCount = batch.size() - batchDuplicateCount;
        lastBatchDuplicateCount = batchDuplicateCount;
    }
    
    /**
     * 设置 PreparedStatement 参数
     */
    protected void setPreparedStatement(java.sql.PreparedStatement ps, MigrationData data, int index) 
            throws java.sql.SQLException {
        ps.setObject(1, data.getId());
        ps.setObject(2, data.getBusinessDate());
        ps.setObject(3, data.getDuplicateKey());
        ps.setString(4, extractField(data.getContent(), "user_name"));
        ps.setDouble(5, Double.parseDouble(extractField(data.getContent(), "amount")));
        ps.setObject(6, 0);
        ps.setString(7, "Migrated");
        ps.setString(8, "MIGRATED");
        ps.setObject(9, new java.util.Date());
        ps.setObject(10, new java.util.Date());
    }
    
    /**
     * 提取 JSON 字段
     */
    private String extractField(String json, String field) {
        try {
            String key = "\"" + field + "\":";
            int startIdx = json.indexOf(key);
            if (startIdx == -1) {
                return null;
            }
            
            int valueStart = startIdx + key.length();
            while (valueStart < json.length() && Character.isWhitespace(json.charAt(valueStart))) {
                valueStart++;
            }
            
            if (valueStart >= json.length()) {
                return null;
            }
            
            char firstChar = json.charAt(valueStart);
            
            if (firstChar == '"') {
                int endIdx = json.indexOf('"', valueStart + 1);
                return endIdx > valueStart ? json.substring(valueStart + 1, endIdx) : null;
            } else if (Character.isDigit(firstChar) || firstChar == '-') {
                int endIdx = valueStart;
                while (endIdx < json.length() && 
                       (Character.isDigit(json.charAt(endIdx)) || 
                        json.charAt(endIdx) == '.' || 
                        json.charAt(endIdx) == '-')) {
                    endIdx++;
                }
                return json.substring(valueStart, endIdx);
            } else {
                return null;
            }
        } catch (Exception e) {
            logger.warn("解析 JSON 字段失败：field={}", field, e);
            return null;
        }
    }
    
    /**
     * 记录单条数据入库异常（到单独的日志文件）
     */
    private void logSingleRecordError(MigrationData data, Exception e) {
        String errorMsg = String.format(
            "入库失败 - id=%d, user_code=%s, business_time=%s, 错误：%s",
            data.getId(),
            data.getDuplicateKey(),
            data.getBusinessDate(),
            e.getMessage()
        );
        
        logger.error(errorMsg, e);

        // 记录到单独的 db-insert-error.log 文件
        // 记录为可执行的 SQL 语句格式，方便后续人工处理
        logAsExecutableSQL(data, e.getMessage());
        
        // 清理 RocksDB 中的 key，达到二次校验通过
        if (rocksDBFilter != null && data.getDuplicateKey() != null) {
            try {
                rocksDBFilter.removeDuplicate(data.getDuplicateKey());
                logger.info("✓ 已清理 RocksDB 中的 key: {}", data.getDuplicateKey());
            } catch (Exception cleanupEx) {
                logger.error("✗ 清理 RocksDB key 失败：{}", data.getDuplicateKey(), cleanupEx);
            }
        }
    }
    
    /**
     * 将失败数据记录为可执行的 SQL 语句格式
     */
    private void logAsExecutableSQL(MigrationData data, String errorMsg) {
        try {
            String userName = extractField(data.getContent(), "user_name");
            String amount = extractField(data.getContent(), "amount");
            
            // 生成 INSERT INTO target_data_error 的 SQL 语句
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("INSERT INTO target_data_error (");
            sqlBuilder.append("id, business_time, user_code, user_name, amount, status, remark, ");
            sqlBuilder.append("source_flag, migrate_time, create_time, error_msg, error_time) VALUES (");
            sqlBuilder.append(data.getId()).append(", ");
            sqlBuilder.append("'").append(data.getBusinessDate()).append("', ");
            sqlBuilder.append("'").append(data.getDuplicateKey()).append("', ");
            sqlBuilder.append("'").append(userName).append("', ");
            sqlBuilder.append(amount).append(", ");
            sqlBuilder.append("0, 'Migrated', 'MIGRATED', ");
            sqlBuilder.append("NOW(), NOW(), ");
            sqlBuilder.append("'").append(escapeSql(errorMsg)).append("', ");
            sqlBuilder.append("NOW()");
            sqlBuilder.append(");");
            
            // 以 SQL 格式记录到日志
            dbErrorLogger.info("-- SQL for manual retry: {}", sqlBuilder.toString());
            
        } catch (Exception ex) {
            dbErrorLogger.error("生成可执行 SQL 失败：id={}", data.getId(), ex);
        }
    }
    
    /**
     * 转义 SQL 字符串中的特殊字符
     */
    private String escapeSql(String str) {
        if (str == null) return "";
        return str.replace("'", "''")  // 单引号转双单引号
                  .replace("\\", "\\\\") // 反斜杠转双反斜杠
                  .replace("\n", "\\n")  // 换行符转义
                  .replace("\r", "\\r"); // 回车符转义
    }
    
    /**
     * 记录失败的数据详情（到单独的日志文件）
     */
    private void logFailedData(List<MigrationData> dataList, Exception rootCause) {
        dbErrorLogger.error("========== 批量插入失败，总数据量：{} ==========", dataList.size(), rootCause);
        for (int i = 0; i < Math.min(10, dataList.size()); i++) {
            MigrationData data = dataList.get(i);
            logger.error("[{}] id={}, user_code={}, content={}",
                    i, data.getId(), data.getDuplicateKey(), data.getContent());
            logSingleRecordError(data, rootCause);
        }
        if (dataList.size() > 10) {
            logger.error("... 还有 {} 条数据", dataList.size() - 10);
        }
        dbErrorLogger.error("======================================");
    }
    
    @Override
    public void close() {
        logger.info("【消费者】关闭，共处理 {} 批次，累计消费 {} 条，失败 {} 条", batchCount, totalConsumed, errorCount);
    }
    
    public long getTotalConsumed() {
        return totalConsumed;
    }
    
    /**
     * 获取最近批次的入库数量（用于监控器统计）
     */
    public long getLastBatchInsertCount() {
        return lastBatchInsertCount;
    }
    
    /**
     * 获取最近批次的重复数量（用于监控器统计）
     */
    public long getLastBatchDuplicateCount() {
        return lastBatchDuplicateCount;
    }
}
