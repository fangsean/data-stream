package com.datastream.migration.enums;

/**
 * 检查点类型枚举
 */
public enum CheckpointType {
    
    /**
     * B 库数据加载到 RocksDB
     */
    TARGET_DATA_LOAD("target-data-load", "B 库数据加载"),
    
    /**
     * A 库数据迁移到 B 库
     */
    SOURCE_DATA_MIGRATE("source-data-migrate", "A 库数据迁移"),
    
    /**
     * A 库与 B 库数据校验
     */
    TARGET_DATA_CHECK("target-data-check", "数据校验");
    
    private final String code;
    private final String description;
    
    CheckpointType(String code, String description) {
        this.code = code;
        this.description = description;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getDescription() {
        return description;
    }
}
