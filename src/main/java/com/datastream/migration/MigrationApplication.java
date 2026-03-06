package com.datastream.migration;

import com.datastream.migration.filter.BloomFilterDuplicateFilter;
import com.datastream.migration.filter.DuplicateFilter;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.scheduler.MigrationExecutor;
import com.datastream.migration.scheduler.MigrationExecutorImpl;
import com.datastream.migration.service.DataProcessService;
import com.datastream.migration.service.DataSourceService;
import com.datastream.migration.service.impl.DataProcessServiceImpl;
import com.datastream.migration.service.impl.DataSourceServiceImpl;
import com.datastream.migration.tracing.ErrorTraceService;
import com.datastream.migration.tracing.impl.ErrorTraceServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.Date;

/**
 * 大数据迁移平台启动类
 * 
 * @author DataStream
 * @version 1.0
 */
@SpringBootApplication
public class MigrationApplication {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationApplication.class);
    
    public static void main(String[] args) {
        SpringApplication.run(MigrationApplication.class, args);
        logger.info("大数据迁移平台启动成功");
    }
    
    /**
     * 创建数据源服务 Bean
     * 
     * @return DataSourceService
     */
    @Bean
    public DataSourceService dataSourceService() {
        return new DataSourceServiceImpl();
    }
    
    /**
     * 创建数据处理服务 Bean
     * 
     * @return DataProcessService
     */
    @Bean
    public DataProcessService dataProcessService() {
        return new DataProcessServiceImpl();
    }
    
    /**
     * 创建去重过滤器 Bean
     * 
     * @return DuplicateFilter
     */
    @Bean
    public DuplicateFilter duplicateFilter() {
        return new BloomFilterDuplicateFilter(createMigrationConfig());
    }
    
    /**
     * 创建错误追溯服务 Bean
     * 
     * @return ErrorTraceService
     */
    @Bean
    public ErrorTraceService errorTraceService() {
        return new ErrorTraceServiceImpl();
    }
    
    /**
     * 创建迁移执行器 Bean
     * 
     * @return MigrationExecutor
     */
    @Bean
    public MigrationExecutor migrationExecutor(DataSourceService dataSourceService,
                                                DataProcessService dataProcessService,
                                                DuplicateFilter duplicateFilter) {
        MigrationExecutorImpl executor = new MigrationExecutorImpl();
        executor.init(createMigrationConfig(), dataSourceService, dataProcessService, duplicateFilter);
        return executor;
    }
    
    /**
     * 创建迁移配置 Bean
     * 实际项目中应从配置文件读取
     * 
     * @return MigrationConfig
     */
    @Bean
    public MigrationConfig createMigrationConfig() {
        return MigrationConfig.builder()
                .taskId("MIGRATION_TASK_001")
                .taskName("A 库到 B 库数据迁移")
                .sourceDbConnection("jdbc:mysql://localhost:3306/source_db?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai")
                .targetDbConnection("jdbc:mysql://localhost:3306/target_db?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai")
                .sourceTable("source_table")
                .targetTable("target_table")
                .duplicateFields("id,duplicate_key")
                .batchSize(10000)
                .startDate(new Date(1640995200000L)) // 2022-01-01
                .endDate(new Date(1672531199000L))   // 2022-12-31
                .threadCount(4)
                .enableBloomFilter(true)
                .expectedInsertions(1000000000L)
                .fpp(0.0001)
                .filterDataPath("data/filter")
                .errorDataPath("data/error")
                .detailedLogging(true)
                .build();
    }
    
    /**
     * 命令行运行器
     * 用于演示迁移任务执行
     * 
     * @param migrationExecutor 迁移执行器
     * @return CommandLineRunner
     */
    @Bean
    public CommandLineRunner commandLineRunner(MigrationExecutor migrationExecutor) {
        return args -> {
            logger.info("========== 大数据迁移平台准备就绪 ==========");
            logger.info("如需执行迁移任务，请调用 migrationExecutor.execute()");
            
            // 示例：执行迁移任务（注释掉，避免误操作）
            // MigrationResult result = migrationExecutor.execute();
            // logger.info("迁移任务执行完成，结果：{}", result);
        };
    }
}
