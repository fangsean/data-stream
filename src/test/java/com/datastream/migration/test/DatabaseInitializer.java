package com.datastream.migration.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据库测试环境初始化工具
 * 
 * 功能：
 * 1. 创建 A 库源表（unit-01）
 * 2. 创建 B 库目标表（unit-02）
 * 3. 初始化测试数据（按时间范围）
 * 
 * @author DataStream
 * @version 1.0
 */
public class DatabaseInitializer {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseInitializer.class);
    
    /**
     * A 库数据源
     */
    private JdbcTemplate sourceJdbcTemplate;
    
    /**
     * B 库数据源
     */
    private JdbcTemplate targetJdbcTemplate;
    
    /**
     * 日期格式化器
     */
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 构造函数 - 初始化两个数据源
     */
    public DatabaseInitializer() {
        // 初始化 A 库数据源
        DataSource sourceDataSource = createDataSource(
                "jdbc:mysql://192.168.0.137:3306/unit-01?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                "root",
                "password"
        );
        this.sourceJdbcTemplate = new JdbcTemplate(sourceDataSource);
        
        // 初始化 B 库数据源
        DataSource targetDataSource = createDataSource(
                "jdbc:mysql://192.168.0.137:3306/unit-02?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai",
                "root",
                "password"
        );
        this.targetJdbcTemplate = new JdbcTemplate(targetDataSource);
        
        logger.info("数据库初始化工具创建完成");
    }
    
    /**
     * 创建数据源
     */
    private DataSource createDataSource(String url, String username, String password) {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("com.mysql.cj.jdbc.Driver");
        dataSource.setUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        return dataSource;
    }
    
    /**
     * 创建 A 库源表
     * 生产场景：原始数据结构
     */
    public void createSourceTable() {
        logger.info("开始创建 A 库源表...");
        
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS `source_data` (" +
            "  `id` bigint(20) NOT NULL COMMENT '主键 ID'," +
            "  `business_time` datetime DEFAULT NULL COMMENT '业务时间'," +
            "  `user_code` varchar(50) DEFAULT NULL COMMENT '用户编码（去重关键字）'," +
            "  `user_name` varchar(100) DEFAULT NULL COMMENT '用户姓名'," +
            "  `amount` decimal(10,2) DEFAULT NULL COMMENT '金额'," +
            "  `status` int(11) DEFAULT NULL COMMENT '状态'," +
            "  `remark` varchar(500) DEFAULT NULL COMMENT '备注'," +
            "  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'," +
            "  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'," +
            "  PRIMARY KEY (`id`)," +
            "  KEY `idx_business_time` (`business_time`)," +
            "  KEY `idx_user_code` (`user_code`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='源数据表（A 库）'";
        
        try {
            sourceJdbcTemplate.execute(createTableSQL);
            logger.info("✓ A 库源表创建成功：source_data");
        } catch (Exception e) {
            logger.error("✗ A 库源表创建失败", e);
            throw new RuntimeException("A 库源表创建失败", e);
        }
    }
    
    /**
     * 创建 B 库目标表
     * 生产场景：目标表结构略有差异（增加了迁移标识字段）
     */
    public void createTargetTable() {
        logger.info("开始创建 B 库目标表...");
        
        String createTableSQL = 
            "CREATE TABLE IF NOT EXISTS `target_data` (" +
            "  `id` bigint(20) NOT NULL COMMENT '主键 ID'," +
            "  `business_time` datetime DEFAULT NULL COMMENT '业务时间'," +
            "  `user_code` varchar(50) DEFAULT NULL COMMENT '用户编码（去重关键字）'," +
            "  `user_name` varchar(100) DEFAULT NULL COMMENT '用户姓名'," +
            "  `amount` decimal(10,2) DEFAULT NULL COMMENT '金额'," +
            "  `status` int(11) DEFAULT NULL COMMENT '状态'," +
            "  `remark` varchar(500) DEFAULT NULL COMMENT '备注'," +
            "  `source_flag` varchar(50) DEFAULT NULL COMMENT '来源标识（新增）'," +
            "  `migrate_time` datetime DEFAULT NULL COMMENT '迁移时间（新增）'," +
            "  `create_time` datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'," +
            "  `update_time` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间'," +
            "  PRIMARY KEY (`id`)," +
            "  UNIQUE KEY `uk_user_code` (`user_code`)," +
            "  KEY `idx_business_time` (`business_time`)" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='目标数据表（B 库）'";
        
        try {
            targetJdbcTemplate.execute(createTableSQL);
            logger.info("✓ B 库目标表创建成功：target_data");
        } catch (Exception e) {
            logger.error("✗ B 库目标表创建失败", e);
            throw new RuntimeException("B 库目标表创建失败", e);
        }
    }
    
    /**
     * 清空表数据
     */
    public void truncateTables() {
        logger.info("开始清空表数据...");
        
        try {
            sourceJdbcTemplate.execute("TRUNCATE TABLE source_data");
            logger.info("✓ A 库源表已清空");
            
            targetJdbcTemplate.execute("TRUNCATE TABLE target_data");
            logger.info("✓ B 库目标表已清空");
        } catch (Exception e) {
            logger.error("✗ 清空表数据失败", e);
        }
    }
    
    /**
     * 初始化测试数据
     * 
     * @param startDate 开始日期（格式：yyyy-MM-dd HH:mm:ss）
     * @param endDate 结束日期（格式：yyyy-MM-dd HH:mm:ss）
     * @param dailyCount 每天生成的数据量
     * @param duplicateRate 重复数据比例（0.0-1.0）
     */
    public void initTestData(String startDate, String endDate, int dailyCount, double duplicateRate) {
        logger.info("==================== 开始初始化测试数据 ====================");
        logger.info("日期范围：{} 到 {}", startDate, endDate);
        logger.info("每天数据量：{} 条", dailyCount);
        logger.info("重复率：{}%", duplicateRate * 100);
        
        try {
            Date start = DATE_FORMAT.parse(startDate);
            Date end = DATE_FORMAT.parse(endDate);
            
            List<Date> dateList = generateDateList(start, end);
            logger.info("生成日期列表，共 {} 天", dateList.size());
            
            int totalRecords = 0;
            
            for (Date businessDate : dateList) {
                logger.info("---------- 处理日期：{} ----------", formatDate(businessDate));
                
                // 生成该日期的数据
                List<Map<String, Object>> dataList = generateDailyData(
                        businessDate, dailyCount, duplicateRate, totalRecords);
                
                // 批量插入到 A 库
                batchInsertToSource(dataList);
                
                totalRecords += dataList.size();
                logger.info("✓ 日期 {} 完成，插入 {} 条数据", formatDate(businessDate), dataList.size());
            }
            
            logger.info("==================== 测试数据初始化完成 ====================");
            logger.info("总数据量：{} 条", totalRecords);
            logger.info("==========================================================");
            
        } catch (Exception e) {
            logger.error("✗ 初始化测试数据失败", e);
            throw new RuntimeException("初始化测试数据失败", e);
        }
    }
    
    /**
     * 生成日期列表
     */
    private List<Date> generateDateList(Date startDate, Date endDate) {
        List<Date> dateList = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        
        while (!calendar.getTime().after(endDate)) {
            dateList.add(calendar.getTime());
            calendar.add(Calendar.DAY_OF_YEAR, 1);
        }
        
        return dateList;
    }
    
    /**
     * 生成单天的测试数据
     */
    private List<Map<String, Object>> generateDailyData(Date businessDate, int count, 
                                                         double duplicateRate, int baseIndex) {
        List<Map<String, Object>> dataList = new ArrayList<>(count);
        Random random = new Random();
        
        for (int i = 0; i < count; i++) {
            Map<String, Object> data = new HashMap<>();
            
            // ID（全局唯一）
            long id = baseIndex + i + 1;
            data.put("id", id);
            
            // 业务时间（当天随机时间）
            Date businessTime = addRandomTime(businessDate, random);
            data.put("business_time", businessTime);
            
            // 用户编码（去重关键字）
            // 根据重复率决定是否使用已存在的 user_code
            String userCode;
            if (duplicateRate > 0 && i < count * duplicateRate && baseIndex > 0) {
                // 重复数据：使用前 N 天的 user_code
                userCode = "USER_" + String.format("%08d", (i % Math.max(1, baseIndex)) + 1);
            } else {
                // 新数据：生成新的 user_code
                userCode = "USER_" + String.format("%08d", id);
            }
            data.put("user_code", userCode);
            
            // 用户姓名
            data.put("user_name", "用户_" + (i + 1));
            
            // 金额（随机）
            double amount = Math.round((Math.random() * 10000) * 100.0) / 100.0;
            data.put("amount", amount);
            
            // 状态（随机 0-3）
            data.put("status", random.nextInt(4));
            
            // 备注
            data.put("remark", "测试数据_" + System.currentTimeMillis());
            
            // 创建时间
            data.put("create_time", new Date());
            
            dataList.add(data);
        }
        
        return dataList;
    }
    
    /**
     * 添加随机时分秒
     */
    private Date addRandomTime(Date date, Random random) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.set(Calendar.HOUR_OF_DAY, random.nextInt(24));
        calendar.set(Calendar.MINUTE, random.nextInt(60));
        calendar.set(Calendar.SECOND, random.nextInt(60));
        return calendar.getTime();
    }
    
    /**
     * 批量插入到 A 库
     * 使用 Spring BatchUpdate 正确方式
     */
    private void batchInsertToSource(List<Map<String, Object>> dataList) {
        String sql = "INSERT INTO source_data " +
                     "(id, business_time, user_code, user_name, amount, status, remark, create_time) " +
                     "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
        
        // 使用 BatchPreparedStatementSetter 进行批量插入
        final int batchSize = 1000;
        
        for (int i = 0; i < dataList.size(); i += batchSize) {
            final int start = i;
            final int end = Math.min(i + batchSize, dataList.size());
            
            sourceJdbcTemplate.batchUpdate(sql, new org.springframework.jdbc.core.BatchPreparedStatementSetter() {
                @Override
                public void setValues(java.sql.PreparedStatement ps, int j) throws SQLException {
                    int index = start + j;
                    Map<String, Object> data = dataList.get(index);
                    ps.setObject(1, data.get("id"));
                    ps.setObject(2, data.get("business_time"));
                    ps.setObject(3, data.get("user_code"));
                    ps.setObject(4, data.get("user_name"));
                    ps.setObject(5, data.get("amount"));
                    ps.setObject(6, data.get("status"));
                    ps.setObject(7, data.get("remark"));
                    ps.setObject(8, data.get("create_time"));
                }
                
                @Override
                public int getBatchSize() {
                    return end - start;
                }
            });
        }
    }
    
    /**
     * 统计 A 库数据量
     */
    public long countSourceData() {
        try {
            return sourceJdbcTemplate.queryForObject("SELECT COUNT(*) FROM source_data", Long.class);
        } catch (Exception e) {
            logger.warn("统计 A 库数据量失败", e);
            return 0;
        }
    }
    
    /**
     * 统计 B 库数据量
     */
    public long countTargetData() {
        try {
            return targetJdbcTemplate.queryForObject("SELECT COUNT(*) FROM target_data", Long.class);
        } catch (Exception e) {
            logger.warn("统计 B 库数据量失败", e);
            return 0;
        }
    }
    
    /**
     * 格式化日期
     */
    private String formatDate(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }
    
    /**
     * 获取 A 库数据源
     */
    public JdbcTemplate getSourceJdbcTemplate() {
        return sourceJdbcTemplate;
    }
    
    /**
     * 获取 B 库数据源
     */
    public JdbcTemplate getTargetJdbcTemplate() {
        return targetJdbcTemplate;
    }
}
