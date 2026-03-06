package com.datastream.migration.filter;

import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * 基于 RocksDB 的去重过滤器集成测试 - 第二部分
 * 
 * 测试场景：
 * 1. 从 RocksDB 加载已有的过滤器（模拟程序重启）
 * 2. 按日期分批生成 A 库数据（模拟 10 亿数据中的一部分）
 * 3. 使用 RocksDB 过滤器进行高性能去重
 * 4. 验证去重效果和性能指标
 * 
 * 目标：验证完整的迁移流程：按日期分批查询 + RocksDB 高性能去重
 * 
 * @author DataStream
 * @version 1.0
 */
public class RocksDBFilterIntegrationTest_Part2 {
    
    private static final Logger logger = LoggerFactory.getLogger(RocksDBFilterIntegrationTest_Part2.class);
    
    /**
     * 测试场景：完整的迁移流程模拟
     * 
     * 步骤：
     * 1. 从 RocksDB 加载已有数据（Part1 已保存）
     * 2. 按日期分批生成 A 库数据
     * 3. 分批查询并处理数据
     * 4. 使用 RocksDB 过滤器去重
     * 5. 统计结果和性能指标
     */
    @Test
    public void testLoadRocksDBFilterAndMigrateByDate() {
        try {
            logger.info("==================== RocksDB 第二部分测试开始：加载并按日期分批迁移 ====================");
            
            // 1. 检查 RocksDB 目录是否存在
            File dbDir = new File("data/test/filter/rocksdb");
            if (!dbDir.exists()) {
                logger.warn("RocksDB 目录不存在，请先运行 Part1 测试");
                logger.info("Response Body: {\"status\":\"skip\",\"message\":\"RocksDB 目录不存在，请先运行 Part1 测试\"}");
                logger.info("HTTP Status: 200 OK");
                return;
            }
            logger.info("步骤 0: 检测到 RocksDB 目录存在，大小：{} bytes", getDirectorySize(dbDir));
            
            // 2. 创建配置
            MigrationConfig config = createTestConfig();
            logger.info("步骤 1: 创建测试配置，task={}", config.getTaskId());
            
            // 3. 创建新过滤器并从 RocksDB 加载
            RocksDBDuplicateFilter filter = new RocksDBDuplicateFilter(config);
            filter.init();
            
            logger.info("步骤 2: 从 RocksDB 加载过滤器...");
            long loadStartTime = System.currentTimeMillis();
            filter.loadFromFile();
            long loadEndTime = System.currentTimeMillis();
            
            long loadDuration = loadEndTime - loadStartTime;
            long size = filter.size();
            
            logger.info("步骤 2 完成：加载 {} 条数据，耗时：{} ms", size, loadDuration);
            assertEquals("应加载约 100 万条数据", 1000000, size);
            
            // 4. 验证加载的数据可用性
            logger.info("步骤 3: 验证加载数据的可用性...");
            MigrationData testData = createTestData(1L, "EXISTING_KEY_0500000", generateBusinessDate(2022, 0, 500000));
            assertTrue("第 50 万条数据应标记为重复", filter.isDuplicate(testData));
            logger.info("  ✓ 验证通过：加载的数据可正常使用");
            
            // 5. 模拟按日期分批迁移 A 库数据
            logger.info("步骤 4: 开始模拟按日期分批迁移 A 库数据...");
            
            // 5.1 定义迁移的日期范围（模拟 7 天的数据）
            Date startDate = generateBusinessDate(2022, 0, 1);   // 2022-01-01
            Date endDate = generateBusinessDate(2022, 0, 7);     // 2022-01-07
            logger.info("  迁移日期范围：{} 到 {}", formatDate(startDate), formatDate(endDate));
            
            // 5.2 统计信息
            long totalRecords = 0;      // 总记录数
            long duplicateCount = 0;    // 重复数据数
            long newCount = 0;          // 新数据数
            
            // 6. 按日期逐个处理
            List<Date> dateList = generateDateList(startDate, endDate);
            logger.info("  需要处理的日期数：{}", dateList.size());
            
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            
            for (Date businessDate : dateList) {
                logger.info("========== 处理日期：{} ==========", formatDate(businessDate));
                
                // 6.1 生成该日期的 A 库数据（模拟查询）
                // 假设每天 10 万条数据
                int dailyDataCount = 100000;
                List<MigrationData> dailyData = generateDailyData(businessDate, dailyDataCount, filter.size());
                
                logger.info("  该日期 A 库数据总量：{} 条", dailyData.size());
                
                // 6.2 分批处理（每批 1 万条）
                int batchSize = 10000;
                int batchCount = 0;
                
                long dayStartTime = System.currentTimeMillis();
                
                for (int offset = 0; offset < dailyData.size(); offset += batchSize) {
                    int endOffset = Math.min(offset + batchSize, dailyData.size());
                    List<MigrationData> batch = dailyData.subList(offset, endOffset);
                    
                    // 6.2.1 执行过滤（性能测试）
                    long filterStart = System.currentTimeMillis();
                    List<MigrationData> filteredBatch = filter.filterDuplicates(batch);
                    long filterEnd = System.currentTimeMillis();
                    
                    // 6.2.2 统计
                    totalRecords += batch.size();
                    duplicateCount += (batch.size() - filteredBatch.size());
                    newCount += filteredBatch.size();
                    
                    batchCount++;
                    
                    // 打印批次进度和性能
                    if (batchCount % 5 == 0) {
                        double batchThroughput = (batch.size() * 1000.0) / (filterEnd - filterStart);
                        logger.info("    批次 {}/{}, 已处理：{} 条，重复：{} 条，新数据：{} 条，速度：{} 条/秒",
                                batchCount, 
                                (dailyData.size() + batchSize - 1) / batchSize,
                                offset + batchSize,
                                duplicateCount,
                                newCount,
                                batchThroughput);
                    }
                }
                
                long dayEndTime = System.currentTimeMillis();
                long dayDuration = dayEndTime - dayStartTime;
                double dayThroughput = (dailyDataCount * 1000.0) / dayDuration;
                
                logger.info("  日期 {} 处理完成，总计：{}, 重复：{}, 新数据：{}, 耗时：{} ms, 速度：{} 条/秒",
                        formatDate(businessDate), dailyData.size(), 
                        dailyData.size() - newCount, newCount, dayDuration, dayThroughput);
            }
            
            // 7. 输出最终统计
            logger.info("==================== 迁移统计 ====================");
            logger.info("总处理数据量：{} 条", totalRecords);
            logger.info("过滤重复数据：{} 条 ({}%)", duplicateCount, (duplicateCount * 100.0) / totalRecords);
            logger.info("新增数据：{} 条 ({}%)", newCount, (newCount * 100.0) / totalRecords);
            logger.info("RocksDB 当前大小：{} 条", filter.size());
            
            // 计算平均性能
            long totalTime = loadEndTime - loadStartTime;  // 包括加载时间
            double avgThroughput = (totalRecords * 1000.0) / totalTime;
            
            logger.info("平均处理速度：{} 条/秒", avgThroughput);
            logger.info("==================================================");
            
            // 8. 验证结果合理性
            assertTrue("应该有数据处理", totalRecords > 0);
            assertTrue("应该有重复数据", duplicateCount > 0);
            assertTrue("应该有新数据", newCount > 0);
            
            // 9. 触发 Compaction 优化
            logger.info("步骤 5: 触发 RocksDB Compaction 优化...");
            filter.saveToFile();
            logger.info("Compaction 完成，新文件大小：{} bytes", getDirectorySize(dbDir));
            
            // 10. 清理资源
            filter.destroy();
            
            logger.info("==================== RocksDB 第二部分测试完成 ====================");
            logger.info("==================== 性能总结 ====================");
            logger.info("总处理数据：{} 条", totalRecords);
            logger.info("过滤重复：{} 条 ({}%)", duplicateCount, (duplicateCount * 100.0) / totalRecords);
            logger.info("新增数据：{} 条", newCount);
            logger.info("平均速度：{} 条/秒", avgThroughput);
            logger.info("RocksDB 最终大小：{} 条", filter.size());
            logger.info("==================================================");
            
            logger.info("Response Body: {" +
                    "\"status\":\"success\"," +
                    "\"totalRecords\":" + totalRecords + "," +
                    "\"duplicateCount\":" + duplicateCount + "," +
                    "\"newCount\":" + newCount + "," +
                    "\"duplicateRate\":" + String.format("%.2f", (duplicateCount * 100.0) / totalRecords) + "," +
                    "\"avgThroughput\":" + String.format("%.2f", avgThroughput) + "," +
                    "\"finalSize\":" + filter.size() + "}");
            logger.info("HTTP Status: 200 OK");
            
        } catch (Exception e) {
            logger.error("测试失败", e);
            fail("testLoadRocksDBFilterAndMigrateByDate 失败：" + e.getMessage());
            logger.info("Response Body: {\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
            logger.info("HTTP Status: 500 Internal Server Error");
        }
    }
    
    /**
     * 测试场景：大批量数据去重性能测试
     * 
     * 验证在百万级数据下的去重性能
     */
    @Test
    public void testLargeScaleDeduplicationPerformance() {
        try {
            logger.info("==================== RocksDB 性能测试：百万级数据去重 ====================");
            
            // 1. 创建配置和过滤器
            MigrationConfig config = createTestConfig();
            RocksDBDuplicateFilter filter = new RocksDBDuplicateFilter(config);
            filter.init();
            
            // 2. 先加载基础数据（模拟 B 库已有 50 万数据）
            int baseDataCount = 500000;
            logger.info("步骤 1: 初始化 B 库已有 {} 条数据...", baseDataCount);
            
            List<MigrationData> baseData = new ArrayList<>(baseDataCount);
            for (int i = 1; i <= baseDataCount; i++) {
                baseData.add(createTestData((long) i, "BASE_" + i, new Date()));
            }
            
            long addStartTime = System.currentTimeMillis();
            filter.addAll(baseData);
            long addEndTime = System.currentTimeMillis();
            
            long addDuration = addEndTime - addStartTime;
            double addThroughput = (baseDataCount * 1000.0) / addDuration;
            
            logger.info("  添加完成，耗时：{} ms, 速度：{} 条/秒", addDuration, addThroughput);
            
            // 3. 准备测试数据（100 万条，包含 50% 重复）
            int testDataCount = 1000000;
            List<MigrationData> testData = new ArrayList<>(testDataCount);
            
            logger.info("步骤 2: 生成 {} 条测试数据（50% 重复）...", testDataCount);
            
            for (int i = 1; i <= testDataCount; i++) {
                if (i % 2 == 0) {
                    // 偶数：重复数据
                    testData.add(createTestData((long) (1000000 + i), "BASE_" + (i / 2), new Date()));
                } else {
                    // 奇数：新数据
                    testData.add(createTestData((long) (2000000 + i), "NEW_" + i, new Date()));
                }
            }
            
            // 4. 执行去重性能测试
            logger.info("步骤 3: 开始去重性能测试...");
            long filterStartTime = System.currentTimeMillis();
            
            List<MigrationData> result = filter.filterDuplicates(testData);
            
            long filterEndTime = System.currentTimeMillis();
            long filterDuration = filterEndTime - filterStartTime;
            double filterThroughput = (testDataCount * 1000.0) / filterDuration;
            
            // 5. 统计结果
            logger.info("步骤 4: 去重完成");
            logger.info("  输入数据：{} 条", testDataCount);
            logger.info("  输出数据：{} 条", result.size());
            logger.info("  过滤重复：{} 条", testDataCount - result.size());
            logger.info("  处理耗时：{} ms", filterDuration);
            logger.info("  处理速度：{} 条/秒", filterThroughput);
            
            // 内存占用
            long memoryUsed = (Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()) / 1024 / 1024;
            logger.info("  内存占用：约 {} MB", memoryUsed);
            
            // 6. 验证结果正确性
            assertEquals("应过滤掉 50 万条重复数据", 500000, result.size());
            
            // 7. 验证性能指标
            assertTrue("写入速度应该大于 1 万条/秒", addThroughput > 10000);
            assertTrue("过滤速度应该大于 1 万条/秒", filterThroughput > 10000);
            logger.info("✓ 性能验证通过：写入和过滤速度 > 10,000 条/秒");
            
            filter.destroy();
            
            logger.info("==================== RocksDB 性能测试完成 ====================");
            logger.info("==================== 性能总结 ====================");
            logger.info("基础数据：{} 条", baseDataCount);
            logger.info("测试数据：{} 条", testDataCount);
            logger.info("写入速度：{} 条/秒", addThroughput);
            logger.info("过滤速度：{} 条/秒", filterThroughput);
            logger.info("内存占用：{} MB", memoryUsed);
            logger.info("==================================================");
            
            logger.info("Response Body: {" +
                    "\"status\":\"success\"," +
                    "\"inputSize\":" + testDataCount + "," +
                    "\"outputSize\":" + result.size() + "," +
                    "\"addThroughput\":" + String.format("%.2f", addThroughput) + "," +
                    "\"filterThroughput\":" + String.format("%.2f", filterThroughput) + "," +
                    "\"memoryMB\":" + memoryUsed + "}");
            logger.info("HTTP Status: 200 OK");
            
        } catch (Exception e) {
            logger.error("性能测试失败", e);
            fail("testLargeScaleDeduplicationPerformance 失败：" + e.getMessage());
            logger.info("Response Body: {\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
            logger.info("HTTP Status: 500 Internal Server Error");
        }
    }
    
    /**
     * 生成指定日期的数据
     */
    private List<MigrationData> generateDailyData(Date businessDate, int count, long baseSeed) {
        List<MigrationData> dataList = new ArrayList<>(count);
        
        for (int i = 1; i <= count; i++) {
            String duplicateKey;
            
            // 30% 的概率与已有数据重复（模拟真实场景）
            if (i % 10 <= 2 && baseSeed > 0) {
                duplicateKey = "EXISTING_KEY_" + String.format("%08d", (i % baseSeed) + 1);
            } else {
                duplicateKey = "A_LIBRARY_" + formatDateCompact(businessDate) + "_" + i;
            }
            
            MigrationData data = createTestData(
                (long) i,
                duplicateKey,
                businessDate
            );
            dataList.add(data);
        }
        
        return dataList;
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
     * 格式化日期
     */
    private String formatDate(Date date) {
        return new SimpleDateFormat("yyyy-MM-dd").format(date);
    }
    
    /**
     * 格式化日期（紧凑格式）
     */
    private String formatDateCompact(Date date) {
        return new SimpleDateFormat("yyyyMMdd").format(date);
    }
    
    /**
     * 生成业务日期
     */
    private Date generateBusinessDate(int year, int month, int dayOffset) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, dayOffset);
        return calendar.getTime();
    }
    
    /**
     * 创建测试数据
     */
    private MigrationData createTestData(Long id, String duplicateKey, Date businessDate) {
        MigrationData data = new MigrationData();
        data.setId(id);
        data.setBusinessDate(businessDate);
        data.setDuplicateKey(duplicateKey);
        data.setSourceFlag("TEST_SOURCE");
        data.setContent("{\"test\":\"content\",\"id\":" + id + "}");
        data.setCreateTime(new Date());
        data.setUpdateTime(new Date());
        return data;
    }
    
    /**
     * 创建测试配置
     */
    private MigrationConfig createTestConfig() {
        return MigrationConfig.builder()
                .taskId("ROCKSDB_PART2_" + System.currentTimeMillis())
                .taskName("RocksDB 第二部分测试")
                .sourceDbConnection("jdbc:mysql://192.168.0.137:3306/unit-01")
                .targetDbConnection("jdbc:mysql://192.168.0.137:3306/unit-02")
                .sourceTable("source_data")
                .targetTable("target_data")
                .duplicateFields("duplicate_key")
                .batchSize(10000)
                .startDate(new Date())
                .endDate(new Date())
                .threadCount(1)
                .enableBloomFilter(false)
                .expectedInsertions(10000000L)
                .fpp(0.001)
                .filterDataPath("data/test/filter")
                .errorDataPath("data/test/error")
                .detailedLogging(false)
                .build();
    }
    
    /**
     * 递归计算目录大小
     */
    private long getDirectorySize(File dir) {
        long size = 0;
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    size += getDirectorySize(file);
                } else {
                    size += file.length();
                }
            }
        }
        return size;
    }
}
