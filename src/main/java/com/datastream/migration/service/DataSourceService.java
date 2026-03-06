package com.datastream.migration.service;

import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import java.util.Date;
import java.util.List;

/**
 * 数据源服务接口
 * 提供从 A 库查询数据的功能
 * 
 * @author DataStream
 * @version 1.0
 */
public interface DataSourceService {
    
    /**
     * 初始化数据源服务
     * 
     * @param config 迁移配置
     */
    void init(MigrationConfig config);
    
    /**
     * 根据日期范围分批查询数据
     * 
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @param offset 偏移量
     * @param limit 每批次数量
     * @return 迁移数据列表
     */
    List<MigrationData> queryByDateRange(Date startDate, Date endDate, int offset, int limit);
    
    /**
     * 根据业务日期查询数据
     * 
     * @param businessDate 业务日期
     * @param offset 偏移量
     * @param limit 每批次数量
     * @return 迁移数据列表
     */
    List<MigrationData> queryByBusinessDate(Date businessDate, int offset, int limit);
    
    /**
     * 获取指定日期的数据总数
     * 
     * @param businessDate 业务日期
     * @return 数据总数
     */
    long countByBusinessDate(Date businessDate);
    
    /**
     * 获取总数据量
     * 
     * @return 总数据量
     */
    long totalCount();
    
    /**
     * 测试数据库连接
     * 
     * @return true-连接成功，false-连接失败
     */
    boolean testConnection();
    
    /**
     * 关闭数据源
     */
    void close();
}
