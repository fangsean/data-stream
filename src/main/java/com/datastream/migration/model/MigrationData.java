package com.datastream.migration.model;

import lombok.Data;
import java.io.Serializable;
import java.util.Date;

/**
 * 迁移数据实体类
 * 用于在 A 库和 B 库之间传输数据
 * 
 * @author DataStream
 * @version 1.0
 */
@Data
public class MigrationData implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    /**
     * 主键 ID
     */
    private Long id;
    
    /**
     * 业务日期（用于分批查询）
     */
    private Date businessDate;
    
    /**
     * 去重关键字段（根据业务定义）
     */
    private String duplicateKey;
    
    /**
     * 数据来源标识
     */
    private String sourceFlag;
    
    /**
     * 数据内容（JSON 格式或其他格式）
     */
    private String content;
    
    /**
     * 创建时间
     */
    private Date createTime;
    
    /**
     * 更新时间
     */
    private Date updateTime;
    
    /**
     * 扩展字段 1
     */
    private String extField1;
    
    /**
     * 扩展字段 2
     */
    private String extField2;
    
    /**
     * 扩展字段 3
     */
    private String extField3;
}
