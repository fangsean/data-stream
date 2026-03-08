package com.datastream.migration.checker;

import com.datastream.migration.checkpoint.CheckpointManager;
import com.datastream.migration.config.MigrationProperties;
import com.datastream.migration.enums.CheckpointType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 数据校验器 - 按天对比 A 库和 B 库数据
 * 支持多线程并发处理和断点续传
 */
public class DataChecker {
    
    private static final Logger logger = LoggerFactory.getLogger(DataChecker.class);
    private static final Logger checkLogger = LoggerFactory.getLogger("com.datastream.migration.check.error");
    
    private final JdbcTemplate sourceJdbc;  // A 库
    private final JdbcTemplate targetJdbc;  // B 库
    private final CheckpointManager checkpointManager;
    private final int threadCount;  // 并发线程数
    
    public DataChecker(JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc, MigrationProperties migrationProperties) {
        this(sourceJdbc, targetJdbc, migrationProperties, 2);  // 默认 2 个线程
    }
    
    public DataChecker(JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc, MigrationProperties migrationProperties, int threadCount) {
        this.sourceJdbc = sourceJdbc;
        this.targetJdbc = targetJdbc;
        this.threadCount = threadCount;
        this.checkpointManager = new CheckpointManager(
            migrationProperties.getCheckDataPath(), 
            CheckpointType.TARGET_DATA_CHECK,  // 使用检查点类型
            migrationProperties.isEnableCheckpoint()
        );

        // 创建断点目录
        try {
            Files.createDirectories(Paths.get(migrationProperties.getCheckDataPath()));
        } catch (Exception e) {
            logger.error("创建断点目录失败", e);
        }
    }
    
    /**
     * 按天校验数据（支持多线程并发）
     */
    public void checkByDay(String startDate, String endDate, boolean resumeFromCheckpoint) {
        try {
            logger.info("开始校验数据...");
            logger.info("校验范围：{} ~ {}", startDate, endDate);
            
            // 1. 加载断点
            checkpointManager.load();
            
            // 2. 生成待校验的日期列表
            List<String> datesToCheck = generateDatesToCheck(startDate, endDate, resumeFromCheckpoint);
            
            if (datesToCheck.isEmpty()) {
                logger.info("✓ 所有日期已校验完成，无需重复处理");
                return;
            }
            
            logger.info("待校验日期：{} 天", datesToCheck.size());
            
            // 3. 多线程并发校验
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CompletionService<CheckResult> completionService = new ExecutorCompletionService<>(executor);
            
            AtomicInteger checkedCount = new AtomicInteger(0);
            AtomicInteger missingCount = new AtomicInteger(0);
            long overallStartTime = System.currentTimeMillis();
            
            // 提交所有任务
            for (String date : datesToCheck) {
                completionService.submit(() -> {
                    CheckResult result = checkSingleDay(date);
                    
                    // 标记为已完成并保存断点
                    checkpointManager.markCompleted(date);
                    checkpointManager.save();  // 每次完成后立即落盘
                    
                    checkedCount.incrementAndGet();
                    missingCount.addAndGet(result.getMissingCount());
                    
                    logger.info("✓ 第 {} 天完成 - A 库：{} 条，B 库：{} 条，缺失：{} 条，进度：{}/{}", 
                            date, result.getSourceCount(), result.getTargetCount(), 
                            result.getMissingCount(), checkedCount.get(), datesToCheck.size());
                    
                    return result;
                });
            }
            
            // 等待所有任务完成
            for (int i = 0; i < datesToCheck.size(); i++) {
                try {
                    completionService.take().get();
                } catch (Exception e) {
                    logger.error("校验任务执行失败", e);
                }
            }
            
            executor.shutdown();
            
            long overallDuration = System.currentTimeMillis() - overallStartTime;
            
            logger.info("\n╔══════════════════════════════════════════════════════╗");
            logger.info("✓ 数据校验完成！");
            logger.info("  - 总天数：{} 天", datesToCheck.size());
            logger.info("  - 已校验：{} 天", checkedCount.get());
            logger.info("  - 总缺失：{} 条", missingCount.get());
            logger.info("  - 总耗时：{} 秒 ({} 分钟)", overallDuration / 1000, overallDuration / 60000.0);
            logger.info("  - 日志文件：logs/check-error.log");
            logger.info("╚══════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            logger.error("✗ 数据校验失败", e);
            throw new RuntimeException("数据校验失败", e);
        } finally {
            // 服务停止时保存断点
            checkpointManager.save();
            logger.info("✓ 服务停止，断点已保存");
        }
    }
    
    /**
     * 生成待校验的日期列表（跳过已完成的）
     */
    private List<String> generateDatesToCheck(String startDate, String endDate, boolean resumeFromCheckpoint) {
        List<String> allDates = new ArrayList<>();
        java.time.LocalDate startLocalDate = java.time.LocalDate.parse(startDate);
        java.time.LocalDate endLocalDate = java.time.LocalDate.parse(endDate);
        
        java.time.LocalDate currentDate = startLocalDate;
        while (!currentDate.isAfter(endLocalDate)) {
            String dateStr = currentDate.toString();
            
            // 如果启用断点续传且该日期已完成，则跳过
            if (resumeFromCheckpoint && checkpointManager.isCompleted(dateStr)) {
                logger.debug("跳过已完成的日期：{}", dateStr);
            } else {
                allDates.add(dateStr);
            }
            
            currentDate = currentDate.plusDays(1);
        }
        
        return allDates;
    }
    
    /**
     * 校验单天数据
     */
    private CheckResult checkSingleDay(String date) {
        CheckResult result = new CheckResult();
        result.setDate(date);
        
        try {
            String startTime = date + " 00:00:00";
            String endTime = date + " 23:59:59";
            
            // 1. 查询 A 库当天所有 user_code
            logger.info("[{}] 正在查询 A 库数据...", date);
            List<String> sourceCodes = querySourceCodes(startTime, endTime);
            result.setSourceCount(sourceCodes.size());
            logger.info("[{}] ✓ A 库数据量：{} 条", date, sourceCodes.size());
            
            // 2. 查询 B 库当天所有 user_code
            logger.info("[{}] 正在查询 B 库数据...", date);
            Set<String> targetCodes = queryTargetCodes(startTime, endTime);
            result.setTargetCount(targetCodes.size());
            logger.info("[{}] ✓ B 库数据量：{} 条", date, targetCodes.size());
            
            // 3. 链条式算法比较：A 存在但 B 不存在
            logger.info("[{}] 正在进行链式对比...", date);
            int missingCount = 0;
            
            for (String sourceCode : sourceCodes) {
                if (!targetCodes.contains(sourceCode)) {
                    logMissingData(date, sourceCode);
                    missingCount++;
                    
                    if (missingCount % 1000 == 0) {
                        logger.info("[{}]   已发现 {} 条缺失数据...", date, missingCount);
                    }
                }
            }
            
            result.setMissingCount(missingCount);
            
            if (missingCount > 0) {
                logger.warn("[{}] ⚠ 发现 {} 条缺失数据，已记录到 logs/check-error.log", date, missingCount);
            } else {
                logger.info("[{}] ✓ 数据完整，无缺失", date);
            }
            
        } catch (Exception e) {
            logger.error("[{}] 校验单日数据失败", date, e);
            throw new RuntimeException("校验失败", e);
        }
        
        return result;
    }
    
    /**
     * 查询 A 库当天的所有 user_code
     */
    private List<String> querySourceCodes(String startTime, String endTime) {
        String sql = "SELECT user_code FROM source_data " +
                     "WHERE business_time BETWEEN ? AND ? " +
                     "ORDER BY user_code";
        
        return sourceJdbc.queryForList(sql, String.class, startTime, endTime);
    }
    
    /**
     * 查询 B 库当天的所有 user_code
     */
    private Set<String> queryTargetCodes(String startTime, String endTime) {
        String sql = "SELECT user_code FROM target_data " +
                     "WHERE business_time BETWEEN ? AND ? " +
                     "ORDER BY user_code";
        
        List<String> targetList = targetJdbc.queryForList(sql, String.class, startTime, endTime);
        return new HashSet<>(targetList);
    }
    
    /**
     * 记录缺失数据到日志
     */
    private void logMissingData(String date, String userCode) {
        try {
            StringBuilder sqlBuilder = new StringBuilder();
            sqlBuilder.append("-- Missing data on ").append(date).append("\n");
            sqlBuilder.append("-- SELECT * FROM source_data WHERE user_code='").append(userCode)
                     .append("' AND DATE(business_time)=DATE('").append(date).append("');\n");
            
            checkLogger.info("MISSING|date={}|user_code={}", date, userCode);
            checkLogger.info("{}", sqlBuilder.toString());
            
        } catch (Exception e) {
            logger.error("记录缺失数据失败：date={}, user_code={}", date, userCode, e);
        }
    }
    
    /**
     * 校验结果
     */
    public static class CheckResult {
        private String date;          // 日期
        private int sourceCount;      // A 库数量
        private int targetCount;      // B 库数量
        private int missingCount;     // 缺失数量
        
        public String getDate() {
            return date;
        }
        
        public void setDate(String date) {
            this.date = date;
        }
        
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
