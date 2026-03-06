package com.datastream.migration.enums;

/**
 * 错误类型枚举
 * 定义迁移过程中可能遇到的错误类型
 * 
 * @author DataStream
 * @version 1.0
 */
public enum ErrorType {
    
    /**
     * 重复数据
     */
    DUPLICATE("DUPLICATE", "重复数据"),
    
    /**
     * 入库失败
     */
    INSERT_ERROR("INSERT_ERROR", "入库异常"),
    
    /**
     * 查询失败
     */
    QUERY_ERROR("QUERY_ERROR", "查询异常"),
    
    /**
     * 数据转换失败
     */
    TRANSFORM_ERROR("TRANSFORM_ERROR", "数据转换异常"),
    
    /**
     * 网络异常
     */
    NETWORK_ERROR("NETWORK_ERROR", "网络异常"),
    
    /**
     * 数据库连接异常
     */
    DB_CONNECTION_ERROR("DB_CONNECTION_ERROR", "数据库连接异常"),
    
    /**
     * 其他错误
     */
    OTHER("OTHER", "其他错误");
    
    private final String code;
    private final String message;
    
    ErrorType(String code, String message) {
        this.code = code;
        this.message = message;
    }
    
    public String getCode() {
        return code;
    }
    
    public String getMessage() {
        return message;
    }
}
