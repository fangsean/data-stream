package com.datastream.migration.checker;

import com.datastream.migration.config.MigrationProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 数据校验器 - 按天对比 A 库和 B 库数据
 */
public class DataChecker {
    
    private static final Logger logger = LoggerFactory.getLogger(DataChecker.class);
    private static final Logger checkLogger = LoggerFactory.getLogger("com.datastream.migration.check.error");
    
    private final JdbcTemplate sourceJdbc;  // A 库
    private final JdbcTemplate targetJdbc;  // B 库
    private final String checkpointDir;

    public DataChecker(JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc, MigrationProperties migrationProperties) {
        this.sourceJdbc = sourceJdbc;
        this.targetJdbc = targetJdbc;
        this.checkpointDir = migrationProperties.getCheckDataPath();

        // 创建断点目录
        try {
            Files.createDirectories(Paths.get(checkpointDir));
        } catch (IOException e) {
            logger.error("创建断点目录失败", e);
        }
    }
    
    /**
     * 按天校验数据
     * 
     * @param startDate 开始日期 yyyy-MM-dd
     * @param endDate 结束日期 yyyy-MM-dd
     * @param resumeFromCheckpoint 是否从断点续传
     */
    public void checkByDay(String startDate, String endDate, boolean resumeFromCheckpoint) {
        try {
            logger.info("开始校验数据...");
            logger.info("校验范围：{} ~ {}", startDate, endDate);
            logger.info("断点续传：{}", resumeFromCheckpoint ? "已启用" : "未启用");
            
            // 解析日期
            java.time.LocalDate startLocalDate = java.time.LocalDate.parse(startDate);
            java.time.LocalDate endLocalDate = java.time.LocalDate.parse(endDate);
            
            // 计算总天数
            long totalDays = java.time.temporal.ChronoUnit.DAYS.between(startLocalDate, endLocalDate) + 1;
            logger.info("将按天校验，总天数：{} 天", totalDays);
            
            // 如果启用断点续传，加载上次的进度
            String lastCheckedDate = null;
            if (resumeFromCheckpoint) {
                lastCheckedDate = loadCheckpoint(startDate);
                if (lastCheckedDate != null) {
                    logger.info("✓ 从断点续传：上次校验到 {}", lastCheckedDate);
                    startLocalDate = java.time.LocalDate.parse(lastCheckedDate).plusDays(1);
                    logger.info("  从 {} 继续校验", startLocalDate);
                }
            }
            
            int checkedCount = 0;
            int missingCount = 0;
            long overallStartTime = System.currentTimeMillis();
            
            // 按天循环校验
            java.time.LocalDate currentDate = startLocalDate;
            int dayCount = 0;
            
            while (!currentDate.isAfter(endLocalDate)) {
                dayCount++;
                String currentDay = currentDate.toString();
                
                logger.info("\n【第 {}/{} 天】正在校验：{}", dayCount, totalDays, currentDay);
                
                // 校验当天数据
                CheckResult result = checkSingleDay(currentDay);
                checkedCount++;
                missingCount += result.getMissingCount();
                
                // 保存断点
                saveCheckpoint(startDate, currentDay);
                
                logger.info("✓ 第 {} 天完成 - A 库：{} 条，B 库：{} 条，缺失：{} 条", 
                        currentDay, result.getSourceCount(), result.getTargetCount(), result.getMissingCount());
                
                // 移动到下一天
                currentDate = currentDate.plusDays(1);
            }
            
            long overallDuration = System.currentTimeMillis() - overallStartTime;
            
            logger.info("╔══════════════════════════════════════════════════════╗");
            logger.info("✓ 数据校验完成！");
            logger.info("  - 总天数：{} 天", totalDays);
            logger.info("  - 已校验：{} 天", checkedCount);
            logger.info("  - 总缺失：{} 条", missingCount);
            logger.info("  - 总耗时：{} 秒 ({} 分钟)", overallDuration / 1000, overallDuration / 60000.0);
            logger.info("  - 日志文件：logs/check-error.log");
            logger.info("╚══════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            logger.error("✗ 数据校验失败", e);
            throw new RuntimeException("数据校验失败", e);
        }
    }
    
    /**
     * 校验单天数据
     */
    private CheckResult checkSingleDay(String date) {
        CheckResult result = new CheckResult();
        
        try {
            String startTime = date + " 00:00:00";
            String endTime = date + " 23:59:59";
            
            // 1. 查询 A 库当天所有 user_code
            logger.info("  正在查询 A 库数据...");
            List<String> sourceCodes = querySourceCodes(date, startTime, endTime);
            result.setSourceCount(sourceCodes.size());
            logger.info("  ✓ A 库数据量：{} 条", sourceCodes.size());
            
            // 2. 查询 B 库当天所有 user_code
            logger.info("  正在查询 B 库数据...");
            Set<String> targetCodes = queryTargetCodes(date, startTime, endTime);
            result.setTargetCount(targetCodes.size());
            logger.info("  ✓ B 库数据量：{} 条", targetCodes.size());
            
            // 3. 链条式算法比较：A 存在但 B 不存在
            logger.info("  正在进行链式对比...");
            int missingCount = 0;
            
            for (String sourceCode : sourceCodes) {
                if (!targetCodes.contains(sourceCode)) {
                    // A 存在但 B 不存在，记录到日志
                    logMissingData(date, sourceCode);
                    missingCount++;
                    
                    // 每 1000 条打印一次进度
                    if (missingCount % 1000 == 0) {
                        logger.info("    已发现 {} 条缺失数据...", missingCount);
                    }
                }
            }
            
            result.setMissingCount(missingCount);
            
            if (missingCount > 0) {
                logger.warn("  ⚠ 发现 {} 条缺失数据，已记录到 logs/check-error.log", missingCount);
            } else {
                logger.info("  ✓ 数据完整，无缺失");
            }
            
        } catch (Exception e) {
            logger.error("校验单日数据失败：{}", date, e);
            throw new RuntimeException("校验失败", e);
        }
        
        return result;
    }
    
    /**
     * 查询 A 库当天的所有 user_code
     */
    private List<String> querySourceCodes(String date, String startTime, String endTime) {
        String sql = "SELECT user_code FROM source_data " +
                     "WHERE business_time BETWEEN ? AND ? " +
                     "ORDER BY user_code";
        
        return sourceJdbc.queryForList(sql, String.class, startTime, endTime);
    }
    
    /**
     * 查询 B 库当天的所有 user_code（使用 Set 提高查找效率）
     */
    private Set<String> queryTargetCodes(String date, String startTime, String endTime) {
        String sql = "SELECT user_code FROM target_data " +
                     "WHERE business_time BETWEEN ? AND ? " +
                     "ORDER BY user_code";
        
        List<String> targetList = targetJdbc.queryForList(sql, String.class, startTime, endTime);
        return new HashSet<>(targetList);  // 转为 Set 提高 O(1) 查找性能
    }
    
    /**
     * 记录缺失数据到日志
     */
    private void logMissingData(String date, String userCode) {
        try {
            // 格式化为可执行的 SQL 语句
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("-- Missing data on ").append(date).append("\n");
            sqlBuilder.append("-- SELECT * FROM source_data WHERE user_code='").append(userCode).append("' AND DATE(business_time)=DATE('").append(date).append("');\n");
            
            checkLogger.info("MISSING|date={}|user_code={}", date, userCode);
            checkLogger.info("{}", sqlBuilder.toString());
            
        } catch (Exception e) {
            logger.error("记录缺失数据失败：date={}, user_code={}", date, userCode, e);
        }
    }
    
    /**
     * 保存断点
     */
    private void saveCheckpoint(String startDate, String checkedDate) {
        try {
            String checkpointFile = checkpointDir + File.separator + 
                                   "check_" + startDate.replace("-", "") + ".checkpoint";
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(checkpointFile))) {
                writer.write(checkedDate);
            }
            
            logger.debug("断点已保存：{} -> {}", checkpointFile, checkedDate);
            
        } catch (IOException e) {
            logger.error("保存断点失败：{}", checkedDate, e);
        }
    }
    
    /**
     * 加载断点
     */
    private String loadCheckpoint(String startDate) {
        try {
            String checkpointFile = checkpointDir + File.separator + 
                                   "check_" + startDate.replace("-", "") + ".checkpoint";
            
            Path path = Paths.get(checkpointFile);
            if (Files.exists(path)) {
                try (BufferedReader reader = Files.newBufferedReader(path)) {
                    String lastDate = reader.readLine();
                    return lastDate;
                }
            }
            
        } catch (IOException e) {
            logger.debug("未找到断点文件或读取失败", e);
        }
        
        return null;
    }
    
    /**
     * 校验结果
     */
    public static class CheckResult {
        private int sourceCount;   // A 库数量
        private int targetCount;   // B 库数量
        private int missingCount;  // 缺失数量
        
        public int getSourceCount() {
            return sourceCount;
        }
        
        public void setSourceCount(int sourceCount) {
            this.sourceCount = sourceCount;
        }
        
        public int getTargetCount() {
            return targetCount;
        }
        
        public void setTargetCount(int targetCount) {
            this.targetCount = targetCount;
        }
        
        public int getMissingCount() {
            return missingCount;
        }
        
        public void setMissingCount(int missingCount) {
            this.missingCount = missingCount;
        }
    }
}
