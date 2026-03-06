package com.datastream.migration.service.impl;

import com.datastream.migration.enums.ErrorType;
import com.datastream.migration.model.ErrorRecord;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import com.datastream.migration.service.DataSourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据源服务实现类
 * 基于 Spring JdbcTemplate 实现从 A 库查询数据
 * 
 * @author DataStream
 * @version 1.0
 */
public class DataSourceServiceImpl implements DataSourceService {
    
    private static final Logger logger = LoggerFactory.getLogger(DataSourceServiceImpl.class);
    
    /**
     * 迁移配置
     */
    private MigrationConfig config;
    
    /**
     * Spring JdbcTemplate - 用于执行 SQL 查询
     */
    private JdbcTemplate jdbcTemplate;
    
    /**
     * 数据源
     */
    private DataSource dataSource;
    
    /**
     * 错误记录处理器
     */
    private ErrorRecordHandler errorRecordHandler;
    
    /**
     * 自定义的 RowMapper - 将 ResultSet 映射为 MigrationData
     */
    private final RowMapper<MigrationData> rowMapper = new RowMapper<MigrationData>() {
        @Override
        public MigrationData mapRow(ResultSet rs, int rowNum) throws SQLException {
            try {
                MigrationData data = new MigrationData();
                data.setId(rs.getLong("id"));
                
                // 处理可能的 NULL 值
                java.sql.Date bizDate = rs.getDate("business_date");
                data.setBusinessDate(bizDate != null ? new Date(bizDate.getTime()) : null);
                
                data.setDuplicateKey(rs.getString("duplicate_key"));
                data.setSourceFlag(rs.getString("source_flag"));
                data.setContent(rs.getString("content"));
                
                java.sql.Timestamp createTime = rs.getTimestamp("create_time");
                data.setCreateTime(createTime != null ? new Date(createTime.getTime()) : null);
                
                java.sql.Timestamp updateTime = rs.getTimestamp("update_time");
                data.setUpdateTime(updateTime != null ? new Date(updateTime.getTime()) : null);
                
                data.setExtField1(rs.getString("ext_field1"));
                data.setExtField2(rs.getString("ext_field2"));
                data.setExtField3(rs.getString("ext_field3"));
                
                return data;
            } catch (SQLException e) {
                logger.error("映射行数据失败，行号：{}", rowNum, e);
                throw e;
            }
        }
    };
    
    /**
     * 初始化数据源服务
     * 
     * @param config 迁移配置
     */
    @Override
    public void init(MigrationConfig config) {
        this.config = config;
        
        try {
            // 创建数据源
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            dataSource.setUrl(config.getSourceDbConnection());
            // 注意：实际项目中应从配置文件读取用户名密码
            dataSource.setUsername("root");
            dataSource.setPassword("password");
            
            this.dataSource = dataSource;
            this.jdbcTemplate = new JdbcTemplate(dataSource);
            this.errorRecordHandler = new ErrorRecordHandler(config);
            
            logger.info("数据源服务初始化完成，连接：{}", config.getSourceDbConnection());
        } catch (Exception e) {
            logger.error("数据源服务初始化失败", e);
            throw new RuntimeException("数据源服务初始化失败", e);
        }
    }
    
    /**
     * 根据日期范围分批查询数据
     * 支持分页查询，避免一次性加载大量数据
     * 
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param offset 偏移量
     * @param limit 每批次数量
     * @return 迁移数据列表
     */
    @Override
    public List<MigrationData> queryByDateRange(Date startDate, Date endDate, int offset, int limit) {
        String sql = "SELECT * FROM " + config.getSourceTable() + 
                     " WHERE business_date BETWEEN ? AND ? " +
                     " ORDER BY business_date, id " +
                     " LIMIT ? OFFSET ?";
        
        try {
            logger.debug("执行查询：日期范围 {} 到 {}, offset={}, limit={}", 
                    startDate, endDate, offset, limit);
            
            List<MigrationData> dataList = jdbcTemplate.query(sql, rowMapper, 
                    new java.sql.Date(startDate.getTime()),
                    new java.sql.Date(endDate.getTime()),
                    limit, offset);
            
            logger.info("查询成功，返回 {} 条数据", dataList.size());
            return dataList;
        } catch (Exception e) {
            logger.error("查询失败，SQL: {}", sql, e);
            recordError(ErrorType.QUERY_ERROR, null, "查询异常：" + e.getMessage(), null);
            throw new RuntimeException("查询失败", e);
        }
    }
    
    /**
     * 根据业务日期查询数据
     * 按天分批查询数据
     * 
     * @param businessDate 业务日期
     * @param offset 偏移量
     * @param limit 每批次数量
     * @return 迁移数据列表
     */
    @Override
    public List<MigrationData> queryByBusinessDate(Date businessDate, int offset, int limit) {
        String sql = "SELECT * FROM " + config.getSourceTable() + 
                     " WHERE DATE(business_date) = DATE(?) " +
                     " ORDER BY id " +
                     " LIMIT ? OFFSET ?";
        
        try {
            logger.debug("执行查询：业务日期 {}, offset={}, limit={}", businessDate, offset, limit);
            
            List<MigrationData> dataList = jdbcTemplate.query(sql, rowMapper,
                    new java.sql.Date(businessDate.getTime()),
                    limit, offset);
            
            logger.info("查询成功，日期：{}, 返回 {} 条数据", businessDate, dataList.size());
            return dataList;
        } catch (Exception e) {
            logger.error("查询失败，SQL: {}", sql, e);
            recordError(ErrorType.QUERY_ERROR, null, "查询异常：" + e.getMessage(), businessDate);
            throw new RuntimeException("查询失败", e);
        }
    }
    
    /**
     * 获取指定日期的数据总数
     * 用于计算分页
     * 
     * @param businessDate 业务日期
     * @return 数据总数
     */
    @Override
    public long countByBusinessDate(Date businessDate) {
        String sql = "SELECT COUNT(*) FROM " + config.getSourceTable() + 
                     " WHERE DATE(business_date) = DATE(?)";
        
        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class, 
                    new java.sql.Date(businessDate.getTime()));
            
            logger.debug("统计日期 {} 的数据量：{}", businessDate, count);
            return count != null ? count : 0L;
        } catch (Exception e) {
            logger.error("统计失败，SQL: {}", sql, e);
            recordError(ErrorType.QUERY_ERROR, null, "统计异常：" + e.getMessage(), businessDate);
            throw new RuntimeException("统计失败", e);
        }
    }
    
    /**
     * 获取总数据量
     * 用于进度跟踪
     * 
     * @return 总数据量
     */
    @Override
    public long totalCount() {
        String sql = "SELECT COUNT(*) FROM " + config.getSourceTable();
        
        try {
            Long count = jdbcTemplate.queryForObject(sql, Long.class);
            logger.info("总数据量：{}", count);
            return count != null ? count : 0L;
        } catch (Exception e) {
            logger.error("统计总数据量失败", e);
            throw new RuntimeException("统计总数据量失败", e);
        }
    }
    
    /**
     * 测试数据库连接
     * 
     * @return true-连接成功，false-连接失败
     */
    @Override
    public boolean testConnection() {
        try {
            jdbcTemplate.queryForObject("SELECT 1", Integer.class);
            logger.info("数据库连接测试成功");
            return true;
        } catch (Exception e) {
            logger.error("数据库连接测试失败", e);
            return false;
        }
    }
    
    /**
     * 关闭数据源
     * 释放资源
     */
    @Override
    public void close() {
        try {
            if (dataSource instanceof DriverManagerDataSource) {
                // DriverManagerDataSource 不需要显式关闭
                logger.info("数据源已关闭");
            }
        } catch (Exception e) {
            logger.warn("关闭数据源失败", e);
        }
    }
    
    /**
     * 记录错误信息
     * 
     * @param errorType 错误类型
     * @param data 迁移数据
     * @param message 错误信息
     * @param businessDate 业务日期
     */
    private void recordError(ErrorType errorType, MigrationData data, String message, Date businessDate) {
        try {
            ErrorRecord errorRecord = new ErrorRecord();
            errorRecord.setId(System.currentTimeMillis());
            errorRecord.setTaskId(config.getTaskId());
            errorRecord.setErrorType(errorType.getCode());
            errorRecord.setOriginalDataId(data != null ? data.getId() : null);
            errorRecord.setDuplicateKey(data != null ? data.getDuplicateKey() : null);
            errorRecord.setErrorMessage(message);
            errorRecord.setDataSnapshot(data != null ? convertToJson(data) : null);
            errorRecord.setOccurTime(new Date());
            errorRecord.setProcessed(false);
            
            errorRecordHandler.recordError(errorRecord);
        } catch (Exception e) {
            logger.error("记录错误失败", e);
        }
    }
    
    /**
     * 将 MigrationData 转换为 JSON 字符串
     * 
     * @param data 迁移数据
     * @return JSON 字符串
     */
    private String convertToJson(MigrationData data) {
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", data.getId());
        jsonMap.put("duplicateKey", data.getDuplicateKey());
        jsonMap.put("businessDate", data.getBusinessDate() != null ? data.getBusinessDate().toString() : null);
        jsonMap.put("sourceFlag", data.getSourceFlag());
        
        StringBuilder sb = new StringBuilder("{");
        for (Map.Entry<String, Object> entry : jsonMap.entrySet()) {
            if (sb.length() > 1) {
                sb.append(",");
            }
            sb.append("\"").append(entry.getKey()).append("\":");
            if (entry.getValue() == null) {
                sb.append("null");
            } else {
                sb.append("\"").append(entry.getValue().toString()).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }
    
    /**
     * 内部类：错误记录处理器
     * 用于处理错误记录的持久化
     */
    private static class ErrorRecordHandler {
        
        private final MigrationConfig config;
        
        public ErrorRecordHandler(MigrationConfig config) {
            this.config = config;
        }
        
        /**
         * 记录错误
         * 
         * @param errorRecord 错误记录
         */
        public void recordError(ErrorRecord errorRecord) {
            // 简单实现，实际项目中应写入数据库或文件
            logger.error("记录错误：task={}, type={}, message={}", 
                    errorRecord.getTaskId(), 
                    errorRecord.getErrorType(), 
                    errorRecord.getErrorMessage());
        }
    }
}
