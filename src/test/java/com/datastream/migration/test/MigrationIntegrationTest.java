package com.datastream.migration.test;

import com.datastream.migration.consumer.DatabaseBatchInsertConsumer;
import com.datastream.migration.engine.MigrationEngine;
import com.datastream.migration.filter.RocksDBDuplicateFilter;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.producer.DatabaseQueryProducer;
import com.datastream.migration.helper.TargetDataLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 生产者 - 消费者模式集成测试（纯 JUnit4 版）
 * <p>
 * 设计理念：
 * 1. 不依赖 Spring 容器，使用纯 JUnit4
 * 2. 统计定时器每 5 秒自动输出进度
 * 3. 资源清理在 @After 中执行
 */
public class MigrationIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(MigrationIntegrationTest.class);
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    private DatabaseInitializer dbInitializer;
    private RocksDBDuplicateFilter duplicateFilter;
    private MigrationEngine engine;
    private MigrationConfig config;

    // 统计定时器
    private ScheduledExecutorService statsScheduler;

    @Before
    public void setUp() throws Exception {
        logger.info("");
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          测试准备：构建运行环境                      ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        // 1. 构建数据库初始化工具
        dbInitializer = new DatabaseInitializer();
        logger.info("✓ [1/6] 数据库初始化工具创建完成");

        // 2. 构建数据库表结构
        dbInitializer.createSourceTable();
        dbInitializer.createTargetTable();
        logger.info("✓ [2/6] A 库源表和 B 库目标表创建完成");

        // 3. 构建迁移配置
        config = createMigrationConfig();
        logger.info("✓ [3/6] 迁移配置创建完成");

        // 4. 构建 RocksDB 过滤器
        duplicateFilter = new RocksDBDuplicateFilter(config);
        duplicateFilter.init();
        logger.info("✓ [4/6] RocksDB 过滤器初始化完成");

        // 5. 构建消费者
        DatabaseBatchInsertConsumer consumer = new DatabaseBatchInsertConsumer(
                dbInitializer.getTargetJdbcTemplate(),
                getInsertSql(),
                null,
                1000
        );
        logger.info("✓ [5/6] 消费者创建完成，批量大小：1000");

        // 6. 构建迁移引擎
        engine = new MigrationEngine(duplicateFilter,
                consumer,
                1024,
                true,
                1000,
                Executors.newSingleThreadExecutor()
        );
        engine.start();
        logger.info("✓ [6/6] 迁移引擎启动完成");

        // 7. 启动统计定时器
        startStatsReporter();
        logger.info("✓ [7/7] 统计定时器已启动");

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          环境构建完成，准备执行测试                  ║");
        logger.info("╚══════════════════════════════════════════════════════╝");
        logger.info("");
    }

    @After
    public void tearDown() throws Exception {
        logger.info("");
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          测试清理                                    ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        // 1. 停止统计定时器
        if (statsScheduler != null) {
            statsScheduler.shutdown();
            try {
                if (!statsScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    statsScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                statsScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("✓ 统计定时器已停止");
        }

        // 2. 停止迁移引擎
        if (engine != null) {
            engine.stop();
            logger.info("✓ 迁移引擎已停止");
        }

        // 3. 保存并销毁过滤器
        if (duplicateFilter != null) {
            duplicateFilter.saveToFile();
            duplicateFilter.close();
            logger.info("✓ RocksDB 过滤器已保存并关闭");
        }

        // 4. 打印最终统计
        logger.info("");
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          最终统计                                    ║");
        logger.info("╠══════════════════════════════════════════════════════╣");
        logger.info("║ A 库源数据量：{} 条", dbInitializer.countSourceData());
        logger.info("║ B 库目标数据量：{} 条", dbInitializer.countTargetData());
        if (duplicateFilter != null) {
            logger.info("║ 过滤器去重：{} 条", duplicateFilter.getDuplicateCount());
        }
        logger.info("╚══════════════════════════════════════════════════════╝");
        logger.info("");
    }

    /**
     * 启动统计定时器
     */
    private void startStatsReporter() {
        logger.info("启动统计定时器，每 5 秒输出一次进度...");

        statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "Migration-Stats-Reporter");
            thread.setDaemon(true);  // 守护线程
            return thread;
        });

        statsScheduler.scheduleAtFixedRate(() -> {
            try {
                reportStats();
            } catch (Exception e) {
                logger.error("统计任务执行失败", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * 报告统计信息
     */
    private void reportStats() {
        if (engine == null) {
            return;
        }

        AtomicLong producedCount = engine.getProducedCount();
        AtomicLong consumedCount = engine.getConsumedCount();

        long produced = producedCount.get();
        long consumed = consumedCount.get();
        long duplicates = 0;

        if (duplicateFilter != null) {
            duplicates = duplicateFilter.getDuplicateCount();
        }

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          实时统计 ({}s)                               ║", System.currentTimeMillis() % 1000);
        logger.info("╠══════════════════════════════════════════════════════╣");
        logger.info("║ 生产：{} 条", produced);
        logger.info("║ 消费：{} 条", consumed);
        logger.info("║ 去重：{} 条", duplicates);
        logger.info("║ 剩余：{} 条", produced - consumed);

        if (produced > 0 && produced == consumed) {
            logger.info("║ 状态：✓ 已完成                              ║");
        } else if (produced > 0) {
            double progress = (consumed * 100.0) / produced;
            logger.info("║ 进度：{}%                            ║", progress);
        } else {
            logger.info("║ 状态：等待数据...                                      ║");
        }

        logger.info("╚══════════════════════════════════════════════════════╝");
    }

    @Test
    public void testMigrationWithDisruptor() throws InterruptedException, ParseException {
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║     测试开始：使用 Disruptor 进行数据迁移              ║");
        logger.info("╚══════════════════════════════════════════════════════╝");
        logger.info("");

        // 1. 先加载 B 库已有数据到 RocksDB（避免重复）
        logger.info("【步骤 1】加载 B 库已有数据到 RocksDB...");
        loadBLibraryDataToRocksDB();
        logger.info("✓ B 库数据加载完成");
        logger.info("");

        // 2. 创建多个生产者（每天一个）
        logger.info("【步骤 2】创建生产者线程...");
        List<Date> dateList = generateDateList(
                DATE_FORMAT.parse("2024-01-01"),
                DATE_FORMAT.parse("2024-01-07")
        );

        List<DatabaseQueryProducer> producers = new ArrayList<>();
        for (int i = 0; i < dateList.size(); i++) {
            Date businessDate = dateList.get(i);
            DatabaseQueryProducer producer = createProducerForDate(businessDate, i);
            producers.add(producer);
            logger.info("  ✓ 创建生产者 {}，负责日期：{}", i, DATE_FORMAT.format(businessDate));
        }
        logger.info("✓ 共创建 {} 个生产者", producers.size());
        logger.info("");

        // 3. 执行迁移
        logger.info("【步骤 3】开始执行数据迁移...");
        engine.execute(new ArrayList<>(producers));
        logger.info("✓ 步骤 3 完成 - 数据迁移执行完毕");
        logger.info("");

        // 4. 验证结果
        logger.info("【步骤 4】验证迁移结果...");
        verifyResults();
        logger.info("✓ 步骤 4 完成 - 结果验证完毕");
    }

    /**
     * 加载 B 库已有数据到 RocksDB
     */
    private void loadBLibraryDataToRocksDB() {
        TargetDataLoader loader = new TargetDataLoader();
        loader.loadToRocksDB(
                dbInitializer.getTargetJdbcTemplate(),
                duplicateFilter,
                "SELECT user_code \n" +
                        "      FROM target_data \n" +
                        "      WHERE business_time >= ? AND business_time < ?",
                "2024-01-01",
                "2024-01-08"
        );
    }

    /**
     * 创建指定日期的生产者
     */
    private DatabaseQueryProducer createProducerForDate(Date businessDate, int index) {
        String sql = getQuerySql();
        Object[] params = new Object[]{new java.sql.Date(businessDate.getTime())};

        return new DatabaseQueryProducer(
                dbInitializer.getSourceJdbcTemplate(),
                sql,
                params,
                DatabaseQueryProducer.createRowMapper("{\"user_name\":\"%s\",\"amount\":%.2f}"),
                duplicateFilter,
                5000
        );
    }

    /**
     * 查询 SQL（可定制）
     */
    protected String getQuerySql() {
        return "SELECT id, business_time, user_code, user_name, amount, status, remark " +
                "FROM source_data " +
                "WHERE DATE(business_time) = DATE(?) " +
                "ORDER BY id " +
                "LIMIT ? OFFSET ?";
    }

    /**
     * 插入 SQL（可定制）
     */
    protected String getInsertSql() {
        return "INSERT INTO target_data " +
                "(id, business_time, user_code, user_name, amount, status, remark, " +
                "source_flag, migrate_time, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    /**
     * 插入Error SQL（可定制）
     */
    protected String getInsertErrorSql() {
        return "INSERT INTO target_data_error " +
                "(id, business_time, user_code, user_name, amount, status, remark, " +
                "source_flag, migrate_time, create_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    }

    /**
     * 验证结果
     */
    private void verifyResults() {
        logger.info("");
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          验证结果                                    ║");
        logger.info("╠══════════════════════════════════════════════════════╣");

        long sourceCount = dbInitializer.countSourceData();
        long targetCount = dbInitializer.countTargetData();
        long duplicateCount = duplicateFilter.getDuplicateCount();

        logger.info("║ A 库源数据量：{} 条", sourceCount);
        logger.info("║ B 库目标数据量：{} 条", targetCount);
        logger.info("║ 过滤器去重：{} 条", duplicateCount);
        logger.info("╚══════════════════════════════════════════════════════╝");

        if (targetCount > 0) {
            logger.info("✓ 验证通过：数据迁移成功");
        } else {
            logger.error("✗ 验证失败：B 库没有数据");
        }
    }

    /**
     * 生成日期列表
     */
    private List<Date> generateDateList(Date start, Date end) {
        List<Date> list = new ArrayList<>();
        java.util.Calendar cal = java.util.Calendar.getInstance();
        cal.setTime(start);
        while (!cal.getTime().after(end)) {
            list.add(cal.getTime());
            cal.add(java.util.Calendar.DAY_OF_YEAR, 1);
        }
        return list;
    }

    /**
     * 创建迁移配置
     */
    private MigrationConfig createMigrationConfig() {
        return MigrationConfig.builder()
                .taskId("TEST_" + System.currentTimeMillis())
                .sourceDbConnection("jdbc:mysql://192.168.0.137:3306/unit-01")
                .targetDbConnection("jdbc:mysql://192.168.0.137:3306/unit-02")
                .duplicateFields("user_code")
                .filterDataPath("data/test/filter003")
                .expectedInsertions(100000L)
                .fpp(0.001)
                .build();
    }
}
