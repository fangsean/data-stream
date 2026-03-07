package com.datastream.migration.enums;

/**
 * 迁移任务模式枚举
 */
public enum MigrationMode {
    
    /**
     * 模式 1：加载 B 库已有数据到 RocksDB
     */
    TARGET_DATA_LOAD("target-data-load", "加载 B 库已有数据到 RocksDB"),
    
    /**
     * 模式 2：A 库数据迁移到 B 库
     */
    SOURCE_DATA_MIGRATE("source-data-migrate", "A 库数据迁移到 B 库"),
    
    /**
     * 模式 3：A 库与 B 库数据链式校验
     */
    TARGET_DATA_CHECK_EXISTING("target-data-check-existing", "A 库与 B 库数据链式校验");
    
    private final String code;
    private final String description;
    
    MigrationMode(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
    
    /**
     * 根据代码获取枚举
     */
    public static MigrationMode fromCode(String code) {
        for (MigrationMode mode : values()) {
            if (mode.getCode().equals(code)) {
                return mode;
            }
        }
        throw new IllegalArgumentException("未知的迁移模式：" + code);
    }
    
    /**
     * 打印所有可用模式
     */
    public static void printAvailableModes() {
        System.out.println("╔══════════════════════════════════════════════════════╗");
        System.out.println("║          可用的迁移模式：                          ║");
        System.out.println("╠══════════════════════════════════════════════════════╣");
        for (MigrationMode mode : values()) {
            System.out.printf("║  %-50s ║%n", mode.getCode() + " - " + mode.getDescription());
        }
        System.out.println("╚══════════════════════════════════════════════════════╝\n");
    }
}
