package com.datastream.migration.service.impl;

import com.datastream.migration.enums.ErrorType;
import com.datastream.migration.model.ErrorRecord;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import com.datastream.migration.service.DataProcessService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 数据处理服务实现类
 * 提供数据转换、验证、清洗和入库功能
 * 
 * @author DataStream
 * @version 1.0
 */
public class DataProcessServiceImpl implements DataProcessService {
    
    private static final Logger logger = LoggerFactory.getLogger(DataProcessServiceImpl.class);
    
    /**
     * 迁移配置
     */
    private MigrationConfig config;
    
    /**
     * 目标数据源
     */
    private DataSource targetDataSource;
    
    /**
     * 目标 JdbcTemplate
     */
    private JdbcTemplate targetJdbcTemplate;
    
    /**
     * 错误记录处理器
     */
    private ErrorRecordHandler errorRecordHandler;
    
    /**
     * 日期格式化工具
     */
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 邮箱验证正则
     */
    private static final Pattern EMAIL_PATTERN = Pattern.compile(
            "^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(\\.[a-zA-Z0-9_-]+)+$");
    
    /**
     * 手机号验证正则
     */
    private static final Pattern PHONE_PATTERN = Pattern.compile("^1[3-9]\\d{9}$");
    
    /**
     * 初始化数据处理服务
     * 
     * @param config 迁移配置
     */
    @Override
    public void init(MigrationConfig config) {
        this.config = config;
        
        try {
            // 创建目标数据源
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
            dataSource.setUrl(config.getTargetDbConnection());
            // 注意：实际项目中应从配置文件读取用户名密码
            dataSource.setUsername("root");
            dataSource.setPassword("password");
            
            this.targetDataSource = dataSource;
            this.targetJdbcTemplate = new JdbcTemplate(dataSource);
            this.errorRecordHandler = new ErrorRecordHandler(config);
            
            logger.info("数据处理服务初始化完成，目标连接：{}", config.getTargetDbConnection());
        } catch (Exception e) {
            logger.error("数据处理服务初始化失败", e);
            throw new RuntimeException("数据处理服务初始化失败", e);
        }
    }
    
    /**
     * 处理单条数据
     * 包括数据转换、验证、清洗等
     * 
     * @param data 待处理的迁移数据
     * @return 处理后的迁移数据
     */
    @Override
    public MigrationData process(MigrationData data) {
        if (data == null) {
            return null;
        }
        
        try {
            // 1. 数据清洗
            MigrationData cleanedData = clean(data);
            
            // 2. 数据转换
            MigrationData transformedData = transform(cleanedData);
            
            // 3. 数据验证
            if (!validate(transformedData)) {
                logger.warn("数据验证失败，ID: {}", data.getId());
                recordError(ErrorType.TRANSFORM_ERROR, data, "数据验证失败");
                return null;
            }
            
            return transformedData;
        } catch (Exception e) {
            logger.error("处理数据失败，ID: {}", data.getId(), e);
            recordError(ErrorType.TRANSFORM_ERROR, data, "处理异常：" + e.getMessage());
            return null;
        }
    }
    
    /**
     * 批量处理数据
     * 
     * @param dataList 待处理的迁移数据列表
     * @return 处理后的迁移数据列表
     */
    @Override
    public List<MigrationData> processBatch(List<MigrationData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<MigrationData> processedList = new ArrayList<>(dataList.size());
        
        for (MigrationData data : dataList) {
            try {
                MigrationData processed = process(data);
                if (processed != null) {
                    processedList.add(processed);
                }
            } catch (Exception e) {
                logger.error("批量处理数据失败，ID: {}", data.getId(), e);
                recordError(ErrorType.TRANSFORM_ERROR, data, "批量处理异常：" + e.getMessage());
            }
        }
        
        logger.info("批量处理完成，输入：{}, 输出：{}", dataList.size(), processedList.size());
        return processedList;
    }
    
    /**
     * 验证数据有效性
     * 可根据业务需求扩展验证规则
     * 
     * @param data 迁移数据
     * @return true-有效，false-无效
     */
    @Override
    public boolean validate(MigrationData data) {
        if (data == null) {
            return false;
        }
        
        // 1. 必填字段验证
        if (data.getId() == null) {
            logger.debug("验证失败：ID 为空");
            return false;
        }
        
        // 2. 去重关键字段验证
        if (data.getDuplicateKey() == null || data.getDuplicateKey().trim().isEmpty()) {
            logger.debug("验证失败：去重关键字为空");
            return false;
        }
        
        // 3. 数据内容验证（如果有）
        if (data.getContent() != null && !data.getContent().trim().isEmpty()) {
            // 可添加 JSON 格式验证等
        }
        
        // 4. 扩展字段验证（根据业务需求）
        // 例如：验证邮箱格式
        if (data.getExtField1() != null && !data.getExtField1().isEmpty()) {
            // 如果 extField1 存储邮箱，可取消注释
            // if (!EMAIL_PATTERN.matcher(data.getExtField1()).matches()) {
            //     logger.debug("验证失败：邮箱格式不正确");
            //     return false;
            // }
        }
        
        // 5. 验证手机号格式
        if (data.getExtField2() != null && !data.getExtField2().isEmpty()) {
            // 如果 extField2 存储手机号，可取消注释
            // if (!PHONE_PATTERN.matcher(data.getExtField2()).matches()) {
            //     logger.debug("验证失败：手机号格式不正确");
            //     return false;
            // }
        }
        
        return true;
    }
    
    /**
     * 数据转换
     * 将源数据格式转换为目标格式
     * 
     * @param data 源数据
     * @return 转换后的数据
     */
    @Override
    public MigrationData transform(MigrationData data) {
        if (data == null) {
            return null;
        }
        
        try {
            MigrationData newData = new MigrationData();
            
            // ID 保持不变
            newData.setId(data.getId());
            
            // 业务日期保持不变
            newData.setBusinessDate(data.getBusinessDate());
            
            // 去重关键字可能需要格式化（去除空格、转小写等）
            if (data.getDuplicateKey() != null) {
                newData.setDuplicateKey(data.getDuplicateKey().trim().toLowerCase());
            }
            
            // 设置来源标识
            newData.setSourceFlag("MIGRATED_FROM_A");
            
            // 内容可能需要转换格式
            newData.setContent(transformContent(data.getContent()));
            
            // 时间字段
            newData.setCreateTime(data.getCreateTime() != null ? data.getCreateTime() : new Date());
            newData.setUpdateTime(new Date());
            
            // 扩展字段根据业务需求转换
            newData.setExtField1(transformField(data.getExtField1()));
            newData.setExtField2(transformField(data.getExtField2()));
            newData.setExtField3(transformField(data.getExtField3()));
            
            return newData;
        } catch (Exception e) {
            logger.error("数据转换失败，ID: {}", data.getId(), e);
            throw new RuntimeException("数据转换失败", e);
        }
    }
    
    /**
     * 数据清洗
     * 去除无效、错误、冗余数据
     * 
     * @param data 待清洗的数据
     * @return 清洗后的数据
     */
    @Override
    public MigrationData clean(MigrationData data) {
        if (data == null) {
            return null;
        }
        
        try {
            // 1. 去除字符串字段的首尾空格
            if (data.getDuplicateKey() != null) {
                data.setDuplicateKey(data.getDuplicateKey().trim());
            }
            
            if (data.getSourceFlag() != null) {
                data.setSourceFlag(data.getSourceFlag().trim());
            }
            
            if (data.getContent() != null) {
                data.setContent(data.getContent().trim());
            }
            
            // 2. 处理 NULL 值
            if (data.getExtField1() != null) {
                data.setExtField1(data.getExtField1().trim());
                if (data.getExtField1().isEmpty()) {
                    data.setExtField1(null);
                }
            }
            
            if (data.getExtField2() != null) {
                data.setExtField2(data.getExtField2().trim());
                if (data.getExtField2().isEmpty()) {
                    data.setExtField2(null);
                }
            }
            
            if (data.getExtField3() != null) {
                data.setExtField3(data.getExtField3().trim());
                if (data.getExtField3().isEmpty()) {
                    data.setExtField3(null);
                }
            }
            
            return data;
        } catch (Exception e) {
            logger.error("数据清洗失败，ID: {}", data.getId(), e);
            throw new RuntimeException("数据清洗失败", e);
        }
    }
    
    /**
     * 保存数据到目标库
     * 
     * @param data 迁移数据
     * @return true-成功，false-失败
     */
    @Override
    public boolean saveToTarget(MigrationData data) {
        if (data == null) {
            return false;
        }
        
        String sql = "INSERT INTO " + config.getTargetTable() + 
                     " (id, business_date, duplicate_key, source_flag, content, " +
                     "create_time, update_time, ext_field1, ext_field2, ext_field3) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try {
            int rows = targetJdbcTemplate.update(sql,
                    data.getId(),
                    data.getBusinessDate() != null ? new java.sql.Date(data.getBusinessDate().getTime()) : null,
                    data.getDuplicateKey(),
                    data.getSourceFlag(),
                    data.getContent(),
                    data.getCreateTime() != null ? new java.sql.Timestamp(data.getCreateTime().getTime()) : null,
                    data.getUpdateTime() != null ? new java.sql.Timestamp(data.getUpdateTime().getTime()) : null,
                    data.getExtField1(),
                    data.getExtField2(),
                    data.getExtField3()
            );
            
            if (rows > 0) {
                logger.debug("保存数据成功，ID: {}", data.getId());
                return true;
            } else {
                logger.warn("保存数据失败，返回行数：0, ID: {}", data.getId());
                return false;
            }
        } catch (Exception e) {
            logger.error("保存数据失败，SQL: {}, ID: {}", sql, data.getId(), e);
            recordError(ErrorType.INSERT_ERROR, data, "入库异常：" + e.getMessage());
            return false;
        }
    }
    
    /**
     * 批量保存数据到目标库
     * 使用批处理方式提高性能
     * 
     * @param dataList 迁移数据列表
     * @return 成功的数量
     */
    @Override
    public int saveBatchToTarget(final List<MigrationData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return 0;
        }
        
        String sql = "INSERT INTO " + config.getTargetTable() + 
                     " (id, business_date, duplicate_key, source_flag, content, " +
                     "create_time, update_time, ext_field1, ext_field2, ext_field3) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        
        try {
            int[] batchResults = targetJdbcTemplate.batchUpdate(sql, 
                    new BatchPreparedStatementSetter() {
                @Override
                public void setValues(PreparedStatement ps, int i) throws SQLException {
                    MigrationData data = dataList.get(i);
                    ps.setLong(1, data.getId());
                    ps.setObject(2, data.getBusinessDate() != null ? new java.sql.Date(data.getBusinessDate().getTime()) : null);
                    ps.setString(3, data.getDuplicateKey());
                    ps.setString(4, data.getSourceFlag());
                    ps.setString(5, data.getContent());
                    ps.setTimestamp(6, data.getCreateTime() != null ? new java.sql.Timestamp(data.getCreateTime().getTime()) : null);
                    ps.setTimestamp(7, data.getUpdateTime() != null ? new java.sql.Timestamp(data.getUpdateTime().getTime()) : null);
                    ps.setString(8, data.getExtField1());
                    ps.setString(9, data.getExtField2());
                    ps.setString(10, data.getExtField3());
                }
                
                @Override
                public int getBatchSize() {
                    return dataList.size();
                }
            });
            
            // 统计成功数量
            int successCount = 0;
            for (int result : batchResults) {
                if (result > 0) {
                    successCount++;
                }
            }
            
            logger.info("批量保存完成，总数：{}, 成功：{}", dataList.size(), successCount);
            return successCount;
        } catch (Exception e) {
            logger.error("批量保存数据失败", e);
            // 记录批量错误
            for (MigrationData data : dataList) {
                recordError(ErrorType.INSERT_ERROR, data, "批量入库异常：" + e.getMessage());
            }
            throw new RuntimeException("批量保存数据失败", e);
        }
    }
    
    /**
     * 关闭服务
     */
    @Override
    public void close() {
        try {
            if (targetDataSource instanceof DriverManagerDataSource) {
                // DriverManagerDataSource 不需要显式关闭
                logger.info("数据处理服务已关闭");
            }
        } catch (Exception e) {
            logger.warn("关闭数据处理服务失败", e);
        }
    }
    
    /**
     * 转换内容字段
     * 可根据业务需求扩展
     * 
     * @param content 原始内容
     * @return 转换后的内容
     */
    private String transformContent(String content) {
        if (content == null || content.trim().isEmpty()) {
            return null;
        }
        
        // 这里可以根据业务需要进行内容转换
        // 例如：JSON 格式转换、编码转换等
        return content.trim();
    }
    
    /**
     * 转换字段值
     * 
     * @param field 字段值
     * @return 转换后的字段值
     */
    private String transformField(String field) {
        if (field == null || field.trim().isEmpty()) {
            return null;
        }
        
        return field.trim();
    }
    
    /**
     * 记录错误信息
     * 
     * @param errorType 错误类型
     * @param data 迁移数据
     * @param message 错误信息
     */
    private void recordError(ErrorType errorType, MigrationData data, String message) {
        try {
            ErrorRecord errorRecord = new ErrorRecord();
            errorRecord.setId(System.currentTimeMillis());
            errorRecord.setTaskId(config.getTaskId());
            errorRecord.setErrorType(errorType.getCode());
            errorRecord.setOriginalDataId(data != null ? data.getId() : null);
            errorRecord.setDuplicateKey(data != null ? data.getDuplicateKey() : null);
            errorRecord.setErrorMessage(message);
            errorRecord.setDataSnapshot(convertToJson(data));
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
        if (data == null) {
            return null;
        }
        
        Map<String, Object> jsonMap = new HashMap<>();
        jsonMap.put("id", data.getId());
        jsonMap.put("duplicateKey", data.getDuplicateKey());
        jsonMap.put("businessDate", data.getBusinessDate() != null ? DATE_FORMAT.format(data.getBusinessDate()) : null);
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
            logger.error("记录错误：task={}, type={}, message={}, originalDataId={}, duplicateKey={}",
                    errorRecord.getTaskId(), 
                    errorRecord.getErrorType(), 
                    errorRecord.getErrorMessage(),
                    errorRecord.getOriginalDataId(),
                    errorRecord.getDuplicateKey());
        }
    }
}
