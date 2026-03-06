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
 * 大数据量去重过滤器集成测试 - 第一部分
 * 
 * 测试场景：
 * 1. 模拟 B 库已有数据（初始化过滤器）
 * 2. 将过滤器保存到磁盘
 * 3. 验证下次启动时可从磁盘加载并正确去重
 * 
 * 目标：验证过滤器的持久化和断点续传功能
 * 
 * @author DataStream
 * @version 1.0
 */
public class DuplicateFilterIntegrationTest_Part1 {
    
    private static final Logger logger = LoggerFactory.getLogger(DuplicateFilterIntegrationTest_Part1.class);
    
    /**
     * 测试场景：模拟 B 库已有 100 万数据，初始化过滤器并保存到磁盘
     * 
     * 步骤：
     * 1. 创建包含 100 万数据的过滤器（模拟 B 库已有数据）
     * 2. 保存过滤器到磁盘
     * 3. 验证文件已生成
     */
    @Test
    public void testInitFilterAndSaveToFile() {
        try {
            logger.info("==================== 第一部分测试开始：初始化过滤器并保存到磁盘 ====================");
            
            // 1. 创建测试配置
            MigrationConfig config = createTestConfig();
            logger.info("步骤 1: 创建测试配置，task={}", config.getTaskId());
            
            // 2. 创建过滤器并初始化
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            logger.info("步骤 2: 过滤器初始化完成");
            
            // 3. 模拟 B 库已有 100 万数据
            int existingDataCount = 1000000;
            logger.info("步骤 3: 开始模拟 B 库已有 {} 条数据...", existingDataCount);
            
            List<MigrationData> existingDataList = new ArrayList<>(existingDataCount);
            for (int i = 1; i <= existingDataCount; i++) {
                MigrationData data = createTestData(
                    (long) i, 
                    "EXISTING_KEY_" + String.format("%08d", i),  // EXISTING_KEY_00000001
                    generateBusinessDate(2022, 0, i)  // 2022-01-01 开始
                );
                existingDataList.add(data);
                
                // 每 10 万条打印一次进度
                if (i % 100000 == 0) {
                    logger.info("  已生成 {} 条 B 库已有数据，进度：{:.2f}%", i, (i * 100.0) / existingDataCount);
                }
            }
            
            // 4. 批量添加到过滤器
            logger.info("步骤 4: 将 {} 条数据添加到过滤器...", existingDataCount);
            long startTime = System.currentTimeMillis();
            filter.addAll(existingDataList);
            long endTime = System.currentTimeMillis();
            logger.info("添加完成，耗时：{} ms, 平均速度：{:.2f} 条/秒", 
                    (endTime - startTime), 
                    (existingDataCount * 1000.0) / (endTime - startTime));
            
            // 5. 验证过滤器中的元素数量
            assertEquals("过滤器中应有 100 万条数据", existingDataCount, filter.size());
            logger.info("步骤 5: 验证通过，过滤器当前元素数量：{}", filter.size());
            
            // 6. 随机抽查验证（确保数据正确添加）
            logger.info("步骤 6: 随机抽查验证...");
            MigrationData sampleData = existingDataList.get(500000);  // 第 50 万条
            assertTrue("第 50 万条数据应标记为重复", filter.isDuplicate(sampleData));
            logger.info("  抽查第 50 万条数据：验证通过");
            
            // 7. 保存过滤器到磁盘
            logger.info("步骤 7: 保存过滤器到磁盘...");
            filter.saveToFile();
            
            // 8. 验证文件已生成
            File filterFile = new File("data/test/filter/filter_data.dat");
            assertTrue("过滤器文件应该存在", filterFile.exists());
            logger.info("步骤 8: 验证通过，过滤器文件已保存：{}, 大小：{} bytes", 
                    filterFile.getAbsolutePath(), filterFile.length());
            
            // 9. 清理内存（模拟程序退出）
            filter.destroy();
            logger.info("步骤 9: 过滤器已销毁（模拟程序退出）");
            
            logger.info("==================== 第一部分测试完成 ====================");
            logger.info("Response Body: {\"status\":\"success\",\"message\":\"过滤器初始化和持久化成功\",\"savedCount\":1000000,\"fileSize\":" + filterFile.length() + "}");
            logger.info("HTTP Status: 200 OK");
            
        } catch (Exception e) {
            logger.error("测试失败", e);
            fail("testInitFilterAndSaveToFile 失败：" + e.getMessage());
            logger.info("Response Body: {\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
            logger.info("HTTP Status: 500 Internal Server Error");
        }
    }
    
    /**
     * 测试场景：新数据去重测试
     * 
     * 验证：
     * 1. 与 B 库已有数据重复的数据应被过滤
     * 2. 新数据应该通过
     */
    @Test
    public void testNewDataDeduplication() {
        try {
            logger.info("==================== 附加测试：新数据去重验证 ====================");
            
            // 1. 创建配置和过滤器
            MigrationConfig config = createTestConfig();
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 2. 先添加一些基础数据（模拟 B 库已有数据）
            logger.info("步骤 1: 添加基础数据（模拟 B 库已有）...");
            for (int i = 1; i <= 10000; i++) {
                filter.add(createTestData((long) i, "BASE_KEY_" + i, new Date()));
            }
            logger.info("  已添加 {} 条基础数据", filter.size());
            
            // 3. 准备测试数据（包含重复和新数据）
            List<MigrationData> testDataList = new ArrayList<>();
            
            // 3.1 重复数据（应与基础数据重复）
            for (int i = 1; i <= 1000; i++) {
                testDataList.add(createTestData((long) (100000 + i), "BASE_KEY_" + i, new Date()));
            }
            
            // 3.2 新数据（不重复）
            for (int i = 1; i <= 5000; i++) {
                testDataList.add(createTestData((long) (200000 + i), "NEW_KEY_" + i, new Date()));
            }
            
            logger.info("步骤 2: 准备测试数据，总数：{} (重复：1000, 新数据：5000)", testDataList.size());
            
            // 4. 执行过滤
            long filterStartTime = System.currentTimeMillis();
            List<MigrationData> filteredData = filter.filterDuplicates(testDataList);
            long filterEndTime = System.currentTimeMillis();
            
            // 5. 验证结果
            logger.info("步骤 3: 过滤完成，耗时：{} ms", (filterEndTime - filterStartTime));
            assertNotNull("过滤结果不应为 null", filteredData);
            assertEquals("应过滤掉 1000 条重复数据，剩余 5000 条", 5000, filteredData.size());
            logger.info("步骤 4: 验证通过，过滤后剩余 {} 条数据", filteredData.size());
            
            // 6. 保存过滤器
            filter.saveToFile();
            File filterFile = new File("data/test/filter/filter_data.dat");
            logger.info("步骤 5: 过滤器已保存，文件大小：{} bytes", filterFile.length());
            
            filter.destroy();
            
            logger.info("==================== 附加测试完成 ====================");
            logger.info("Response Body: {\"status\":\"success\",\"inputSize\":6000,\"filteredSize\":5000,\"duplicateCount\":1000}");
            logger.info("HTTP Status: 200 OK");
            
        } catch (Exception e) {
            logger.error("测试失败", e);
            fail("testNewDataDeduplication 失败：" + e.getMessage());
            logger.info("Response Body: {\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
            logger.info("HTTP Status: 500 Internal Server Error");
        }
    }
    
    /**
     * 生成业务日期
     * 
     * @param year 年
     * @param month 月（0-11）
     * @param dayOffset 日期偏移（从当月 1 日开始）
     * @return Date
     */
    private Date generateBusinessDate(int year, int month, int dayOffset) {
        Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.YEAR, year);
        calendar.set(Calendar.MONTH, month);
        calendar.set(Calendar.DAY_OF_MONTH, dayOffset);
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        return calendar.getTime();
    }
    
    /**
     * 创建测试数据
     * 
     * @param id 数据 ID
     * @param duplicateKey 去重关键字
     * @param businessDate 业务日期
     * @return MigrationData
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
     * 
     * @return MigrationConfig
     */
    private MigrationConfig createTestConfig() {
        return MigrationConfig.builder()
                .taskId("PART1_TEST_" + System.currentTimeMillis())
                .taskName("第一部分测试 - 过滤器初始化")
                .sourceDbConnection("jdbc:mysql://localhost:3306/test_source")
                .targetDbConnection("jdbc:mysql://localhost:3306/test_target")
                .sourceTable("test_source_table")
                .targetTable("test_target_table")
                .duplicateFields("duplicate_key")
                .batchSize(10000)
                .startDate(new Date())
                .endDate(new Date())
                .threadCount(1)
                .enableBloomFilter(true)
                .expectedInsertions(10000000L)  // 1000 万预期
                .fpp(0.001)  // 0.1% 误判率
                .filterDataPath("data/test/filter")
                .errorDataPath("data/test/error")
                .detailedLogging(false)
                .build();
    }
}
