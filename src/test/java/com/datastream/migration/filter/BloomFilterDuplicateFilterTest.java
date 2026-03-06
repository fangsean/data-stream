package com.datastream.migration.filter;

import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.*;

/**
 * BloomFilter 去重过滤器单元测试
 * 测试要求：
 * 1. 使用 Mockito 框架
 * 2. 每个测试方法独立 Mock，禁用 setUp 通用配置
 * 3. 只 mock 真正会被调用的方法
 * 4. 每个测试方法用 try-catch 捕获异常
 * 5. 测试结束打印结果
 * 
 * @author DataStream
 * @version 1.0
 */
public class BloomFilterDuplicateFilterTest {
    
    private static final Logger logger = LoggerFactory.getLogger(BloomFilterDuplicateFilterTest.class);
    
    /**
     * 测试初始化方法
     */
    @Test
    public void testInit() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器并初始化
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 验证初始化成功（不抛出异常）
            assertNotNull("过滤器应该初始化成功", filter);
            
            logger.info("testInit 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testInit 执行失败", e);
            fail("testInit 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试单条数据去重
     */
    @Test
    public void testIsDuplicate() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 创建测试数据
            MigrationData data1 = createTestData(1L, "KEY_001");
            MigrationData data2 = createTestData(2L, "KEY_002");
            
            // 第一次检查不应重复
            boolean isDuplicate1 = filter.isDuplicate(data1);
            assertFalse("第一次检查不应重复", isDuplicate1);
            
            // 添加到过滤器
            filter.add(data1);
            
            // 第二次检查应重复
            boolean isDuplicate2 = filter.isDuplicate(data1);
            assertTrue("第二次检查应重复", isDuplicate2);
            
            // 不同的 key 不应重复
            boolean isDuplicate3 = filter.isDuplicate(data2);
            assertFalse("不同的 key 不应重复", isDuplicate3);
            
            filter.destroy();
            
            logger.info("testIsDuplicate 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testIsDuplicate 执行失败", e);
            fail("testIsDuplicate 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试批量过滤去重
     */
    @Test
    public void testFilterDuplicates() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 创建测试数据列表
            List<MigrationData> dataList = new ArrayList<>();
            for (int i = 1; i <= 100; i++) {
                MigrationData data = createTestData((long) i, "KEY_" + String.format("%03d", i));
                dataList.add(data);
            }
            
            // 添加一些重复数据
            for (int i = 1; i <= 10; i++) {
                MigrationData duplicateData = createTestData((long) (1000 + i), "KEY_" + String.format("%03d", i));
                dataList.add(duplicateData);
            }
            
            // 执行批量过滤
            List<MigrationData> filteredList = filter.filterDuplicates(dataList);
            
            // 验证结果
            assertNotNull("过滤后的列表不应为空", filteredList);
            assertEquals("原始数据应为 110 条", 110, dataList.size());
            assertEquals("过滤后应为 100 条", 100, filteredList.size());
            
            filter.destroy();
            
            logger.info("testFilterDuplicates 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testFilterDuplicates 执行失败", e);
            fail("testFilterDuplicates 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试添加单条数据
     */
    @Test
    public void testAdd() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 创建测试数据
            MigrationData data = createTestData(1L, "KEY_ADD_TEST");
            
            // 添加数据
            filter.add(data);
            
            // 验证数据已添加
            assertEquals("元素数量应为 1", 1, filter.size());
            
            // 再次检查应重复
            assertTrue("数据应标记为重复", filter.isDuplicate(data));
            
            filter.destroy();
            
            logger.info("testAdd 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testAdd 执行失败", e);
            fail("testAdd 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试批量添加数据
     */
    @Test
    public void testAddAll() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 创建测试数据列表
            List<MigrationData> dataList = new ArrayList<>();
            for (int i = 1; i <= 50; i++) {
                dataList.add(createTestData((long) i, "KEY_BATCH_" + i));
            }
            
            // 批量添加
            filter.addAll(dataList);
            
            // 验证数量
            assertEquals("元素数量应为 50", 50, filter.size());
            
            filter.destroy();
            
            logger.info("testAddAll 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testAddAll 执行失败", e);
            fail("testAddAll 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试保存和加载文件
     */
    @Test
    public void testSaveAndLoadFromFile() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 添加一些数据
            List<MigrationData> dataList = new ArrayList<>();
            for (int i = 1; i <= 20; i++) {
                dataList.add(createTestData((long) i, "KEY_SAVE_" + i));
            }
            filter.addAll(dataList);
            
            // 保存到文件
            filter.saveToFile();
            
            // 创建新过滤器并加载
            BloomFilterDuplicateFilter newFilter = new BloomFilterDuplicateFilter(config);
            newFilter.init();
            newFilter.loadFromFile();
            
            // 验证加载的数据
            assertEquals("加载后元素数量应为 20", 20, newFilter.size());
            
            // 清理测试文件
            File filterFile = new File("data/filter/filter_data.dat");
            if (filterFile.exists()) {
                filterFile.delete();
            }
            
            filter.destroy();
            newFilter.destroy();
            
            logger.info("testSaveAndLoadFromFile 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testSaveAndLoadFromFile 执行失败", e);
            fail("testSaveAndLoadFromFile 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试清空过滤器
     */
    @Test
    public void testClear() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 添加数据
            filter.add(createTestData(1L, "KEY_CLEAR_TEST"));
            assertEquals("添加后元素数量应为 1", 1, filter.size());
            
            // 清空
            filter.clear();
            
            // 验证已清空
            assertEquals("清空后元素数量应为 0", 0, filter.size());
            
            filter.destroy();
            
            logger.info("testClear 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testClear 执行失败", e);
            fail("testClear 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试空数据处理
     */
    @Test
    public void testNullAndEmptyData() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 测试 null 数据
            filter.add(null);
            assertFalse("null 数据不应标记为重复", filter.isDuplicate(null));
            
            // 测试空列表
            List<MigrationData> emptyList = new ArrayList<>();
            List<MigrationData> result = filter.filterDuplicates(emptyList);
            assertNotNull("空列表过滤结果不应为 null", result);
            assertTrue("空列表过滤结果应为空", result.isEmpty());
            
            filter.destroy();
            
            logger.info("testNullAndEmptyData 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testNullAndEmptyData 执行失败", e);
            fail("testNullAndEmptyData 失败：" + e.getMessage());
        }
    }
    
    /**
     * 测试并发安全性
     */
    @Test
    public void testConcurrency() {
        try {
            // 创建测试配置
            MigrationConfig config = createTestConfig();
            
            // 创建过滤器
            final BloomFilterDuplicateFilter filter = new BloomFilterDuplicateFilter(config);
            filter.init();
            
            // 创建多个线程并发添加数据
            Thread[] threads = new Thread[10];
            for (int i = 0; i < threads.length; i++) {
                final int threadId = i;
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        for (int j = 0; j < 100; j++) {
                            long index = threadId * 100 + j;
                            filter.add(createTestData(index, "KEY_CONCURRENT_" + index));
                        }
                    }
                });
                threads[i].start();
            }
            
            // 等待所有线程完成
            for (Thread thread : threads) {
                thread.join();
            }
            
            // 验证总数
            assertEquals("并发添加后元素数量应为 1000", 1000, filter.size());
            
            filter.destroy();
            
            logger.info("testConcurrency 执行完成 - PASSED");
        } catch (Exception e) {
            logger.error("testConcurrency 执行失败", e);
            fail("testConcurrency 失败：" + e.getMessage());
        }
    }
    
    /**
     * 创建测试配置
     * 
     * @return MigrationConfig
     */
    private MigrationConfig createTestConfig() {
        return MigrationConfig.builder()
                .taskId("TEST_TASK_001")
                .taskName("测试任务")
                .sourceDbConnection("jdbc:mysql://localhost:3306/test_source")
                .targetDbConnection("jdbc:mysql://localhost:3306/test_target")
                .sourceTable("test_source_table")
                .targetTable("test_target_table")
                .duplicateFields("id,duplicate_key")
                .batchSize(1000)
                .startDate(new Date())
                .endDate(new Date())
                .threadCount(2)
                .enableBloomFilter(true)
                .expectedInsertions(10000L)
                .fpp(0.001)
                .filterDataPath("data/test/filter")
                .errorDataPath("data/test/error")
                .detailedLogging(false)
                .build();
    }
    
    /**
     * 创建测试数据
     * 
     * @param id 数据 ID
     * @param duplicateKey 去重关键字
     * @return MigrationData
     */
    private MigrationData createTestData(Long id, String duplicateKey) {
        MigrationData data = new MigrationData();
        data.setId(id);
        data.setBusinessDate(new Date());
        data.setDuplicateKey(duplicateKey);
        data.setSourceFlag("TEST");
        data.setContent("{\"test\":\"content\"}");
        data.setCreateTime(new Date());
        data.setUpdateTime(new Date());
        return data;
    }
}
