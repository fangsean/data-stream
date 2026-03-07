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
 * 基于 RocksDB 的去重过滤器集成测试 - 第一部分
 * 
 * 测试场景：
 * 1. 模拟 B 库已有数据（初始化 RocksDB）
 * 2. 利用 RocksDB 的高性能写入特性
 * 3. 验证下次启动时可从 RocksDB 加载并正确去重
 * 
 * 目标：验证 RocksDB 过滤器的持久化和高性能
 * 
 * @author DataStream
 * @version 1.0
 */
public class RocksDBFilterIntegrationTest_Part1 {
    
    private static final Logger logger = LoggerFactory.getLogger(RocksDBFilterIntegrationTest_Part1.class);
    
    /**
     * 测试场景：模拟 B 库已有 100 万数据，初始化 RocksDB 并验证性能
     * 
     * 步骤：
     * 1. 创建包含 100 万数据的 RocksDB 过滤器
     * 2. 验证 RocksDB 的高性能写入
     * 3. 验证数据可正确查询
     */
    @Test
    public void testInitRocksDBFilterAndPerformance() {
        try {
            logger.info("==================== RocksDB 第一部分测试开始：高性能初始化 ====================");
            
            // 1. 创建测试配置
            MigrationConfig config = createTestConfig();
            logger.info("步骤 1: 创建测试配置，task={}", config.getTaskId());
            
            // 2. 创建 RocksDB 过滤器并初始化
            RocksDBDuplicateFilter filter = new RocksDBDuplicateFilter(config);
            filter.init();
            logger.info("步骤 2: RocksDB 过滤器初始化完成");
            
            // 3. 模拟 B 库已有 100 万数据
            int existingDataCount = 1000000;
            logger.info("步骤 3: 开始模拟 B 库已有 {} 条数据...", existingDataCount);
            
            List<MigrationData> existingDataList = new ArrayList<>(existingDataCount);
            for (int i = 1; i <= existingDataCount; i++) {
                MigrationData data = createTestData(
                    (long) i, 
                    "EXISTING_KEY_" + String.format("%08d", i),
                    generateBusinessDate(2022, 0, i)
                );
                existingDataList.add(data);
                
                if (i % 100000 == 0) {
                    logger.info("  已生成 {} 条 B 库已有数据，进度：{}%", i, (i * 100.0) / existingDataCount);
                }
            }
            
            // 4. 批量添加到 RocksDB（性能测试）
            logger.info("步骤 4: 批量添加 {} 条数据到 RocksDB...", existingDataCount);
            long startTime = System.currentTimeMillis();
            filter.addAll(existingDataList);
            long endTime = System.currentTimeMillis();
            
            long duration = endTime - startTime;
            double throughput = (existingDataCount * 1000.0) / duration;
            
            logger.info("步骤 4 完成：添加耗时：{} ms, 平均速度：{} 条/秒", duration, throughput);
            
            // 5. 验证性能指标
            assertTrue("写入速度应该大于 1 万条/秒", throughput > 10000);
            logger.info("✓ 性能验证通过：写入速度 > 10,000 条/秒");
            
            // 6. 验证数据量
            long size = filter.size();
            assertEquals("RocksDB 中应有 100 万条数据", existingDataCount, size);
            logger.info("步骤 5: 验证通过，RocksDB 当前元素数量：{}", size);
            
            // 7. 随机抽查验证
            logger.info("步骤 6: 随机抽查验证...");
            MigrationData sampleData1 = existingDataList.get(0);  // 第 1 条
            MigrationData sampleData2 = existingDataList.get(500000);  // 第 50 万条
            MigrationData sampleData3 = existingDataList.get(999999);  // 最后一条
            
            assertTrue("第 1 条数据应标记为重复", filter.isDuplicate(sampleData1));
            assertTrue("第 50 万条数据应标记为重复", filter.isDuplicate(sampleData2));
            assertTrue("最后一条数据应标记为重复", filter.isDuplicate(sampleData3));
            
            logger.info("  ✓ 抽查第 1 条、第 50 万条、最后一条数据：验证通过");
            
            // 8. 触发 Compaction 优化
            logger.info("步骤 7: 触发 RocksDB Compaction 优化...");
            filter.saveToFile();
            logger.info("  Compaction 完成");
            
            // 9. 验证数据库目录
            File dbDir = new File("data/test/filter/rocksdb");
            assertTrue("RocksDB 目录应该存在", dbDir.exists());
            
            // 统计目录大小
            long dirSize = getDirectorySize(dbDir);
            logger.info("步骤 8: RocksDB 目录大小：{} bytes ({} MB)", dirSize, dirSize / 1024.0 / 1024.0);
            
            // 计算压缩率
            long uncompressedSize = (long) existingDataCount * 50;  // 估算每条 50 字节
            double compressionRatio = (1.0 - (double) dirSize / uncompressedSize) * 100;
            logger.info("估算压缩率：{}%", compressionRatio);
            
            // 10. 清理资源
            filter.destroy();
            logger.info("步骤 9: RocksDB 过滤器已销毁");
            
            logger.info("==================== RocksDB 第一部分测试完成 ====================");
            logger.info("==================== 性能总结 ====================");
            logger.info("数据量：{} 条", existingDataCount);
            logger.info("写入耗时：{} ms", duration);
            logger.info("写入速度：{} 条/秒", throughput);
            logger.info("存储空间：{} MB", dirSize / 1024.0 / 1024.0);
            logger.info("估算压缩率：{}%", compressionRatio);
            logger.info("==================================================");
            
            logger.info("Response Body: {" +
                    "\"status\":\"success\"," +
                    "\"count\":" + existingDataCount + "," +
                    "\"durationMs\":" + duration + "," +
                    "\"throughput\":" + String.format("%.2f", throughput) + "," +
                    "\"storageSizeMB\":" + String.format("%.2f", dirSize / 1024.0 / 1024.0) + "}");
            logger.info("HTTP Status: 200 OK");
            
        } catch (Exception e) {
            logger.error("测试失败", e);
            fail("testInitRocksDBFilterAndPerformance 失败：" + e.getMessage());
            logger.info("Response Body: {\"status\":\"error\",\"message\":\"" + e.getMessage() + "\"}");
            logger.info("HTTP Status: 500 Internal Server Error");
        }
    }
    
    /**
     * 测试场景：新数据去重测试
     * 
     * 验证：
     * 1. 与 RocksDB 中已有数据重复的数据应被过滤
     * 2. 新数据应该通过
     */
    @Test
    public void testNewDataDeduplication() {
        try {
            logger.info("==================== RocksDB 附加测试：新数据去重验证 ====================");
            
            // 1. 创建配置和过滤器
            MigrationConfig config = createTestConfig();
            RocksDBDuplicateFilter filter = new RocksDBDuplicateFilter(config);
            filter.init();
            
            // 2. 先添加基础数据（模拟 B 库已有）
            logger.info("步骤 1: 添加基础数据（模拟 B 库已有）...");
            int baseCount = 10000;
            for (int i = 1; i <= baseCount; i++) {
                filter.add(createTestData((long) i, "BASE_KEY_" + i, new Date()));
            }
            logger.info("  已添加 {} 条基础数据", filter.size());
            
            // 3. 准备测试数据
            List<MigrationData> testDataList = new ArrayList<>();
            
            // 3.1 重复数据
            for (int i = 1; i <= 1000; i++) {
                testDataList.add(createTestData((long) (100000 + i), "BASE_KEY_" + i, new Date()));
            }
            
            // 3.2 新数据
            for (int i = 1; i <= 5000; i++) {
                testDataList.add(createTestData((long) (200000 + i), "NEW_KEY_" + i, new Date()));
            }
            
            logger.info("步骤 2: 准备测试数据，总数：{} (重复：1000, 新数据：5000)", testDataList.size());
            
            // 4. 执行过滤（性能测试）
            long filterStartTime = System.currentTimeMillis();
            List<MigrationData> filteredData = filter.filterDuplicates(testDataList);
            long filterEndTime = System.currentTimeMillis();
            
            long filterDuration = filterEndTime - filterStartTime;
            double filterThroughput = (testDataList.size() * 1000.0) / filterDuration;
            
            logger.info("步骤 3: 过滤完成，耗时：{} ms, 速度：{} 条/秒", 
                    filterDuration, filterThroughput);
            
            // 5. 验证结果
            assertNotNull("过滤结果不应为 null", filteredData);
            assertEquals("应过滤掉 1000 条重复数据，剩余 5000 条", 5000, filteredData.size());
            logger.info("步骤 4: 验证通过，过滤后剩余 {} 条数据", filteredData.size());
            
            // 6. 验证性能
            assertTrue("过滤速度应该大于 1 万条/秒", filterThroughput > 10000);
            logger.info("✓ 性能验证通过：过滤速度 > 10,000 条/秒");
            
            filter.destroy();
            
            logger.info("==================== RocksDB 附加测试完成 ====================");
            logger.info("Response Body: {\"status\":\"success\",\"inputSize\":6000,\"filteredSize\":5000,\"duplicateCount\":1000,\"throughput\":" + String.format("%.2f", filterThroughput) + "}");
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
                .taskId("ROCKSDB_PART1_" + System.currentTimeMillis())
                .taskName("RocksDB 第一部分测试")
                .sourceDbConnection("jdbc:mysql://192.168.0.137:3306/unit-01")
                .targetDbConnection("jdbc:mysql://192.168.0.137:3306/unit-02")
                .sourceTable("source_data")
                .targetTable("target_data")
                .duplicateFields("duplicate_key")
                .batchSize(10000)
                .startDate(new Date())
                .endDate(new Date())
                .threadCount(1)
                .enableBloomFilter(true)
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
