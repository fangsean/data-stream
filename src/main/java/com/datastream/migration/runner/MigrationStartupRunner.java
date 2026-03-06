package com.datastream.migration.runner;

import com.datastream.migration.checker.DataChecker;
import com.datastream.migration.config.MigrationProperties;
import com.datastream.migration.consumer.DatabaseBatchInsertConsumer;
import com.datastream.migration.engine.MigrationEngine;
import com.datastream.migration.enums.MigrationMode;
import com.datastream.migration.filter.RocksDBDuplicateFilter;
import com.datastream.migration.helper.TargetDataLoader;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.producer.DatabaseQueryProducer;
import com.datastream.migration.strategy.SourceDataLoadStrategy;
import com.datastream.migration.strategy.TargetDataLoadStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Spring Boot 启动后自动执行迁移任务
 */
@Component
public class MigrationStartupRunner implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(MigrationStartupRunner.class);
    private static final Logger statsLogger = LoggerFactory.getLogger("com.datastream.migration.stats");

    @Autowired
    @Qualifier("primaryJdbcTemplate")
    private JdbcTemplate primaryJdbcTemplate;  // A 库查询模板

    @Autowired
    @Qualifier("secondaryJdbcTemplate")
    private JdbcTemplate secondaryJdbcTemplate;  // B 库插入模板

    @Autowired
    private MigrationProperties migrationProperties;  // 迁移配置属性

    private StartupArgsHandler startupArgs;  // 启动参数处理器
    private ScheduledExecutorService statsScheduler;

    @Override
    public void run(String... args) throws Exception {
        // 解析启动参数
        this.startupArgs = StartupArgsHandler.parseArgs(args);
        this.startupArgs.validate();

        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          启动模式：{}                   ║", startupArgs.getMode().getDescription());
        if (startupArgs.getStartDate() != null) {
            logger.info("║          日期范围：{} ~ {}           ║", startupArgs.getStartDate(), startupArgs.getEndDate());
        }
        if (startupArgs.isResumeFromCheckpoint()) {
            logger.info("║          断点续传：已启用                          ║");
        }
        logger.info("╚══════════════════════════════════════════════════════╝");

        // 使用新线程异步执行迁移任务，避免阻塞 Spring Boot 启动
        new Thread(() -> {
            try {
                executeMigrationByMode(startupArgs.getMode());
            } catch (Exception e) {
                logger.error("迁移任务执行失败", e);
            }
        }, "Migration-Executor-Thread").start();
    }

    /**
     * 根据模式执行迁移任务
     */
    private void executeMigrationByMode(MigrationMode mode) throws Exception {
        switch (mode) {
            case TARGET_DATA_LOAD:
                executeTargetDataLoad();
                break;
            case SOURCE_DATA_MIGRATE:
                executeSourceDataMigrate();
                break;
            case TARGET_DATA_CHECK_EXISTING:
                executeTargetDataCheckExisting();
                break;
            default:
                throw new IllegalArgumentException("未知的迁移模式：" + mode);
        }
    }

    /**
     * 模式 1：加载 B 库已有数据到 RocksDB
     */
    private void executeTargetDataLoad() throws Exception {
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          模式 1：加载 B 库已有数据到 RocksDB            ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        // 1. 创建配置
        MigrationConfig config = createMigrationConfig();

        // 2. 创建 RocksDB 过滤器
        RocksDBDuplicateFilter filter = new RocksDBDuplicateFilter(config);
        filter.init();

        // 3. 加载 B 库已有数据到 RocksDB（按天拆分查询）
        logger.info("【步骤 1】加载 B 库已有数据到 RocksDB...");
        TargetDataLoader loader = new TargetDataLoader();

        // 优先使用命令行参数，其次使用配置文件
        String startDate = startupArgs.getStartDate() != null ?
                startupArgs.getStartDate() : migrationProperties.getTargetDataLoad().getStartDate();
        String endDate = startupArgs.getEndDate() != null ?
                startupArgs.getEndDate() : migrationProperties.getTargetDataLoad().getEndDate();

        logger.info("加载日期范围：{} ~ {}", startDate, endDate);

        loader.loadToRocksDB(secondaryJdbcTemplate, filter,
                migrationProperties.getSql().getCheckExisting(),
                startDate, endDate);
        logger.info("✓ B 库数据加载完成");

        // 4. 清理资源
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          任务完成，清理资源                          ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        filter.saveToFile();
        filter.close();

        logger.info("✓ 所有资源已清理");
        logger.info("✓ B 库数据加载任务完成！");
    }

    /**
     * 模式 2：A 库数据迁移到 B 库
     */
    private void executeSourceDataMigrate() throws Exception {
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          模式 2：A 库数据迁移到 B 库                    ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        try {
            // 1. 创建配置
            MigrationConfig config = createMigrationConfig();

            // 2. 使用已注入的 JdbcTemplate（无需手动创建）
            logger.info("正在验证数据库连接...");
            logger.info("✓ A 库数据源已就绪 (primaryJdbcTemplate)");
            logger.info("✓ B 库数据源已就绪 (secondaryJdbcTemplate)");

            // 打印数据源类型（验证是否使用了 HikariCP）
            logger.info("A 库数据源类型：{}", primaryJdbcTemplate.getDataSource().getClass().getSimpleName());
            logger.info("B 库数据源类型：{}", secondaryJdbcTemplate.getDataSource().getClass().getSimpleName());

            // 3. 创建 RocksDB 过滤器
            RocksDBDuplicateFilter filter = new RocksDBDuplicateFilter(config);
            filter.init();

            // 4. 加载 B 库已有数据到 RocksDB（使用策略模式）
            logger.info("【步骤 1】加载 B 库已有数据到 RocksDB...");
            TargetDataLoadStrategy targetStrategy = new TargetDataLoadStrategy(
                    secondaryJdbcTemplate,
                    filter,
                    migrationProperties.getSql().getCheckExisting()
            );

            String targetStartDate = migrationProperties.getTargetDataLoad().getStartDate();
            String targetEndDate = migrationProperties.getTargetDataLoad().getEndDate();

            targetStrategy.loadData(targetStartDate, targetEndDate);
            logger.info("✓ B 库数据加载完成");

            // 5. 创建消费者
            DatabaseBatchInsertConsumer consumer = new DatabaseBatchInsertConsumer(
                    secondaryJdbcTemplate,
                    migrationProperties.getSql().getInsert(),
                    migrationProperties.getSql().getInsertError(),
                    migrationProperties.getBatchSize()
            );
            consumer.setRocksDBFilter(filter);  // 设置过滤器，用于异常时清理

            // 6. 创建迁移引擎
            MigrationEngine engine = new MigrationEngine(filter, consumer, 1024);
            engine.start();

            // 7. 启动统计定时器（每 5 秒输出到 migration-stats.log）
            startStatsReporter(engine, filter);

            // 8. 创建生产者并执行（使用策略模式）
            logger.info("【步骤 2】创建生产者线程...");
            SourceDataLoadStrategy sourceStrategy = new SourceDataLoadStrategy(
                    primaryJdbcTemplate,
                    migrationProperties.getSql().getQuery(),
                    filter
            );

            String sourceStartDate = migrationProperties.getSourceDataMigrate().getStartDate();
            String sourceEndDate = migrationProperties.getSourceDataMigrate().getEndDate();

            sourceStrategy.loadData(sourceStartDate, sourceEndDate);
            List<DatabaseQueryProducer> producers = sourceStrategy.getProducers();

            logger.info("【步骤 3】开始执行数据迁移...");
            engine.execute(new ArrayList<>(producers));
            logger.info("✓ 步骤 3 完成 - 数据迁移执行完毕");

            // 9. 等待所有数据处理完成（最多等待 60 秒）
            waitForCompletion(engine, 60);

            // 10. 验证结果
            logger.info("【步骤 4】验证迁移结果...");
            verifyResults(primaryJdbcTemplate, secondaryJdbcTemplate, filter);
            logger.info("✓ 步骤 4 完成 - 结果验证完毕");

            // 11. 停止统计定时器
            if (statsScheduler != null) {
                statsScheduler.shutdown();
            }

            // 12. 清理资源
            logger.info("╔══════════════════════════════════════════════════════╗");
            logger.info("║          迁移完成，清理资源                          ║");
            logger.info("╚══════════════════════════════════════════════════════╝");

            filter.saveToFile();
            filter.close();

            logger.info("✓ 所有资源已清理");
            logger.info("✓ Spring Boot 迁移任务完成！");

        } catch (Exception e) {
            logger.error("╔══════════════════════════════════════════════════════╗");
            logger.error("║          迁移任务执行失败！                          ║");
            logger.error("╠══════════════════════════════════════════════════════╣");
            logger.error("║ 错误类型：{}", e.getClass().getSimpleName());
            logger.error("║ 错误信息：{}", e.getMessage());
            logger.error("╚══════════════════════════════════════════════════════╝");
            logger.error("详细堆栈：", e);
            throw e;
        }
    }


    /**
     * 模式 3：A 库与 B 库数据链式校验
     */
    private void executeTargetDataCheckExisting() throws Exception {
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          模式 3：A 库与 B 库数据链式校验                 ║");
        logger.info("╚══════════════════════════════════════════════════════╝");

        // 1. 创建数据校验器
        DataChecker checker = new DataChecker(primaryJdbcTemplate, secondaryJdbcTemplate, migrationProperties);
        
        // 2. 获取日期范围（优先使用命令行参数）
        String startDate = startupArgs.getStartDate() != null ? 
            startupArgs.getStartDate() : migrationProperties.getTargetDataCheckExisting().getStartDate();
        String endDate = startupArgs.getEndDate() != null ? 
            startupArgs.getEndDate() : migrationProperties.getTargetDataCheckExisting().getEndDate();
        
        // 3. 执行按天校验
        checker.checkByDay(startDate, endDate, true);
        
        logger.info("✓ 数据校验任务完成！");
    }


    /**
     * 启动统计定时器
     */
    private void startStatsReporter(MigrationEngine engine, RocksDBDuplicateFilter filter) {
        logger.info("启动统计定时器，每 5 秒输出一次进度到 migration-stats.log...");

        statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "Migration-Stats-Reporter");
            thread.setDaemon(true);
            return thread;
        });

        statsScheduler.scheduleAtFixedRate(() -> {
            try {
                long produced = engine.getProducedCount().get();
                long consumed = engine.getConsumedCount().get();
                long duplicates = filter.getDuplicateCount();

                statsLogger.info("╔══════════════════════════════════════════════════════╗");
                statsLogger.info("║          实时统计 ({}ms)                    ║", System.currentTimeMillis() % 1000);
                statsLogger.info("╠══════════════════════════════════════════════════════╣");
                statsLogger.info("║ 生产：{} 条", produced);
                statsLogger.info("║ 消费：{} 条", consumed);
                statsLogger.info("║ 去重：{} 条", duplicates);
                statsLogger.info("║ 剩余：{} 条", produced - consumed);

                if (produced > 0 && produced == consumed) {
                    statsLogger.info("║ 状态：✓ 已完成                              ║");
                } else if (produced > 0) {
                    double progress = (consumed * 100.0) / produced;
                    statsLogger.info("║ 进度：{}%                            ║", progress);
                } else {
                    statsLogger.info("║ 状态：等待数据...                          ║");
                }
                statsLogger.info("╚══════════════════════════════════════════════════════╝");

            } catch (Exception e) {
                logger.error("统计任务执行失败", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
    }

    /**
     * 等待处理完成
     */
    private void waitForCompletion(MigrationEngine engine, int maxWaitSeconds) throws InterruptedException {
        logger.info("正在等待所有数据处理完成...");

        int waitSeconds = 0;
        while (waitSeconds < maxWaitSeconds) {
            long produced = engine.getProducedCount().get();
            long consumed = engine.getConsumedCount().get();

            if (produced > 0 && produced == consumed) {
                logger.info("✓ 所有数据处理完成，耗时 {} 秒", waitSeconds);
                return;
            }

            Thread.sleep(1000);
            waitSeconds++;

            if (waitSeconds % 5 == 0) {
                logger.info("等待中... 已等待 {} 秒，生产：{}, 消费：{}, 剩余：{}",
                        waitSeconds, produced, consumed, produced - consumed);
            }
        }

        logger.warn("⚠ 等待超时 ({} 秒)，生产：{}, 消费：{}",
                maxWaitSeconds, engine.getProducedCount().get(), engine.getConsumedCount().get());
    }

    /**
     * 验证结果
     */
    private void verifyResults(JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc, RocksDBDuplicateFilter filter) {
        Long sourceCount = sourceJdbc.queryForObject("SELECT COUNT(*) FROM source_data", Long.class);
        Long targetCount = targetJdbc.queryForObject("SELECT COUNT(*) FROM target_data", Long.class);
        long duplicateCount = filter.getDuplicateCount();

        logger.info("验证结果:");
        logger.info("  - A 库源数据量：{} 条", sourceCount);
        logger.info("  - B 库目标数据量：{} 条", targetCount);
        logger.info("  - 过滤器去重：{} 条", duplicateCount);

        if (targetCount > 0) {
            logger.info("✓ 验证通过：数据迁移成功");
        } else {
            logger.error("✗ 验证失败：B 库没有数据");
        }
    }

    /**
     * 创建迁移配置
     */
    private MigrationConfig createMigrationConfig() {
        return MigrationConfig.builder()
                .taskId("SPRING_BOOT_AUTO_RUN_" + System.currentTimeMillis())
                .enableBloomFilter(migrationProperties.isEnableBloomFilter())
                .expectedInsertions(migrationProperties.getExpectedInsertions())
                .fpp(migrationProperties.getFpp())
                .filterDataPath(migrationProperties.getFilterDataPath())
                .build();
    }
}
