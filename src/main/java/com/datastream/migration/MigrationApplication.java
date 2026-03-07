package com.datastream.migration;

import com.datastream.migration.runner.StartupArgsHandler;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

/**
 * 数据迁移服务启动类
 */
@SpringBootApplication
@ComponentScan(basePackages = "com.datastream.migration")
public class MigrationApplication {
    
    public static void main(String[] args) {
        StartupArgsHandler.parseArgs(args);
        SpringApplication.run(MigrationApplication.class, args);
    }
}
