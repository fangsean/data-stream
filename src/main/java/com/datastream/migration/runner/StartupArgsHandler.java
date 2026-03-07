package com.datastream.migration.runner;

import com.datastream.migration.enums.MigrationMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 启动参数处理器
 */
public class StartupArgsHandler {
    
    private static final Logger logger = LoggerFactory.getLogger(StartupArgsHandler.class);
    
    private MigrationMode mode;
    private String startDate;
    private String endDate;
    private boolean resumeFromCheckpoint;
    
    /**
     * 解析启动参数
     */
    public static StartupArgsHandler parseArgs(String[] args) {
        if (args.length == 0) {
            printUsage();
            System.exit(1);
        }
        
        StartupArgsHandler handler = new StartupArgsHandler();
        
        // 第一个参数：模式
        try {
            handler.mode = MigrationMode.fromCode(args[0]);
        } catch (IllegalArgumentException e) {
            logger.error("错误：未知的迁移模式 '{}'", args[0]);
            MigrationMode.printAvailableModes();
            System.exit(1);
        }
        
        // 解析后续参数
        for (int i = 1; i < args.length; i++) {
            String arg = args[i];
            
            if (arg.startsWith("--start-date=")) {
                handler.startDate = arg.substring("--start-date=".length());
            } else if (arg.startsWith("--end-date=")) {
                handler.endDate = arg.substring("--end-date=".length());
            } else if (arg.equals("--resume")) {
                handler.resumeFromCheckpoint = true;
            } else if (arg.equals("--help") || arg.equals("-h")) {
                printUsage();
                System.exit(0);
            }
        }
        
        return handler;
    }
    
    /**
     * 打印使用说明
     */
    public static void printUsage() {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║          数据迁移服务 - 使用说明                   ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║ 用法：java -jar data-stream.jar <模式> [选项]       ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║ 必填参数：                                          ║");
        System.out.println("║   <模式>              选择以下一种模式：             ║");
        MigrationMode.printAvailableModes();
        System.out.println("║ 可选参数：                                          ║");
        System.out.println("║   --start-date=YYYY-MM-DD  开始日期                 ║");
        System.out.println("║   --end-date=YYYY-MM-DD    结束日期                 ║");
        System.out.println("║   --resume                从断点续传               ║");
        System.out.println("║   --help, -h              显示帮助信息             ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        System.out.println("║ 示例：                                              ║");
        System.out.println("║   java -jar data-stream.jar target-data-load \\      ║");
        System.out.println("║       --start-date=2020-01-01 \\                     ║");
        System.out.println("║       --end-date=2025-12-31                         ║");
        System.out.println("║                                                     ║");
        System.out.println("║   java -jar data-stream.jar source-data-migrate \\   ║");
        System.out.println("║       --start-date=2024-01-01 \\                     ║");
        System.out.println("║       --end-date=2024-01-07 \\                       ║");
        System.out.println("║       --resume                                      ║");
        System.out.println("║                                                     ║");
        System.out.println("║   java -jar data-stream.jar target-data-check-existing \\  ║");
        System.out.println("║       --start-date=2020-01-01 \\                     ║");
        System.out.println("║       --end-date=2025-12-31                         ║");
        System.out.println("╚══════════════════════════════════════════════════════╝\n");
    }
    
    /**
     * 验证参数是否完整
     */
    public void validate() {
        if (mode == null) {
            throw new IllegalArgumentException("必须指定迁移模式");
        }
        
        // 如果配置文件没有启用，则使用命令行参数
        if (startDate == null || endDate == null) {
            logger.warn("未在命令行中指定日期范围，将使用配置文件中的日期");
        }
    }
    
    // Getters
    
    public MigrationMode getMode() {
        return mode;
    }
    
    public String getStartDate() {
        return startDate;
    }
    
    public String getEndDate() {
        return endDate;
    }
    
    public boolean isResumeFromCheckpoint() {
        return resumeFromCheckpoint;
    }
}
