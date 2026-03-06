package com.datastream.migration.helper;

import com.datastream.migration.filter.DuplicateFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * B 库数据加载器 - 将 B 库已有数据加载到 RocksDB
 * 
 * 优化方案：
 * 1. 按天拆分查询，避免单次查询数据量过大
 * 2. 使用流式查询避免内存溢出
 * 3. 分批提交减少 RocksDB 写入次数
 */
public class TargetDataLoader {
    
    private static final Logger logger = LoggerFactory.getLogger(TargetDataLoader.class);
    
    // 每批处理数量
    private static final int BATCH_SIZE = 5000;
    
    // 进度报告间隔
    private static final int PROGRESS_INTERVAL = 50000;
    
    // 日期格式化器
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    /**
     * 加载 B 库数据到 RocksDB（按天拆分查询）
     * 
     * @param targetJdbc B 库 JDBC
     * @param filter RocksDB 过滤器
     * @param querySql 查询 SQL（必须包含时间区间参数：WHERE business_time >= ? AND business_time < ?)
     * @param startDate 开始日期（yyyy-MM-dd）
     * @param endDate 结束日期（yyyy-MM-dd）
     */
    public void loadToRocksDB(JdbcTemplate targetJdbc, DuplicateFilter filter, 
                              String querySql, String startDate, String endDate) {
        try {
            logger.info("开始加载 B 库已有数据到 RocksDB...");
            logger.info("日期范围：{} ~ {}", startDate, endDate);
            logger.info("配置参数：BATCH_SIZE={}, PROGRESS_INTERVAL={}", BATCH_SIZE, PROGRESS_INTERVAL);
            
            // 解析日期
            LocalDate startLocalDate = LocalDate.parse(startDate, DATE_FORMATTER);
            LocalDate endLocalDate = LocalDate.parse(endDate, DATE_FORMATTER);
            
            // 计算总天数
            long totalDays = java.time.temporal.ChronoUnit.DAYS.between(startLocalDate, endLocalDate) + 1;
            logger.info("将按天拆分查询，总天数：{} 天", totalDays);
            
            int totalCount = 0;
            long overallStartTime = System.currentTimeMillis();
            
            // 按天循环查询
            LocalDate currentDate = startLocalDate;
            int dayCount = 0;
            
            while (!currentDate.isAfter(endLocalDate)) {
                dayCount++;
                String currentDay = currentDate.format(DATE_FORMATTER);
                
                // 构建当天的时间区间
                String startTime = currentDate.atStartOfDay().format(DATETIME_FORMATTER);
                String endTime = currentDate.atTime(23, 59, 59).format(DATETIME_FORMATTER);
                
                logger.info("【第 {}/{} 天】正在查询：{} ({} ~ {})",
                        dayCount, totalDays, currentDay, startTime, endTime);
                
                // 查询当天数据
                int dayTotal = loadSingleDay(targetJdbc, filter, querySql, startTime, endTime);
                totalCount += dayTotal;
                
                logger.info("✓ 第 {} 天完成，加载 {} 条，累计 {} 条", currentDay, dayTotal, totalCount);
                
                // 移动到下一天
                currentDate = currentDate.plusDays(1);
            }
            
            long overallEndTime = System.currentTimeMillis();
            long overallDuration = overallEndTime - overallStartTime;
            
            logger.info("╔══════════════════════════════════════════════════════╗");
            logger.info("✓ B 库数据加载完成！");
            logger.info("  - 总天数：{} 天", totalDays);
            logger.info("  - 总数据量：{} 条", totalCount);
            logger.info("  - 总耗时：{} 秒 ({} 分钟)", overallDuration / 1000, overallDuration / 60000.0);
            logger.info("  - 平均速度：{} 条/秒", (totalCount * 1000.0) / overallDuration);
            logger.info("  - 平均每天：{} 条", totalCount * 1.0 / totalDays);
            logger.info("╚══════════════════════════════════════════════════════╝");
            
        } catch (Exception e) {
            logger.error("✗ B 库数据加载失败", e);
            throw new RuntimeException("B 库数据加载失败", e);
        }
    }
    
    /**
     * 加载单天数据到 RocksDB
     * 
     * @param targetJdbc B 库 JDBC
     * @param filter RocksDB 过滤器
     * @param querySql 查询 SQL
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return 加载的数据量
     */
    private int loadSingleDay(JdbcTemplate targetJdbc, DuplicateFilter filter, 
                              String querySql, String startTime, String endTime) {
        try {
            // 使用流式查询，按时间区间分批加载
            return targetJdbc.query(querySql, rs -> {
                int count = 0;
                long dayStartTime = System.currentTimeMillis();
                
                while (rs.next()) {
                    String userCode = rs.getString(1);
                    
                    if (userCode != null && !userCode.trim().isEmpty()) {
                        // 添加到 RocksDB
                        if (filter instanceof com.datastream.migration.filter.RocksDBDuplicateFilter) {
                            ((com.datastream.migration.filter.RocksDBDuplicateFilter) filter).addDuplicate(userCode.trim());
                        }
                        count++;
                        
                        // 定期报告进度（针对大数据量天）
                        if (count % PROGRESS_INTERVAL == 0) {
                            long elapsed = System.currentTimeMillis() - dayStartTime;
                            double speed = (count * 1000.0) / elapsed;  // 条/秒
                            logger.info("  进度：{} 条，耗时 {} 秒，速度：{} 条/秒",
                                    count, elapsed / 1000, speed);
                        }
                    }
                }
                
                long dayDuration = System.currentTimeMillis() - dayStartTime;
                if (count > 0) {
                    logger.info("  单日统计 - 数据量：{} 条，耗时：{} 秒，速度：{} 条/秒",
                            count, dayDuration / 1000, (count * 1000.0) / dayDuration);
                }
                
                return count;
            }, startTime, endTime);  // 传入时间参数
            
        } catch (Exception e) {
            logger.error("加载单日数据失败：{} ~ {}", startTime, endTime, e);
            throw new RuntimeException("加载单日数据失败", e);
        }
    }
}
