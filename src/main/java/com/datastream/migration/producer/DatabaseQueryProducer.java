package com.datastream.migration.producer;

import com.datastream.migration.filter.DuplicateFilter;
import com.datastream.migration.model.MigrationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

/**
 * 数据库查询生产者
 */
public class DatabaseQueryProducer implements DataProducer {
    
    private static final Logger logger = LoggerFactory.getLogger(DatabaseQueryProducer.class);
    
    private final JdbcTemplate sourceJdbc;
    private final String querySql;
    private final Object[] queryParams;
    private final RowMapper<MigrationData> rowMapper;
    private final DuplicateFilter duplicateFilter;
    private final int batchSize;
    
    private int offset = 0;
    private boolean hasMore = true;
    private int batchCount = 0;
    
    public DatabaseQueryProducer(JdbcTemplate sourceJdbc, 
                                  String querySql, 
                                  Object[] queryParams,
                                  RowMapper<MigrationData> rowMapper,
                                  DuplicateFilter duplicateFilter,
                                  int batchSize) {
        this.sourceJdbc = sourceJdbc;
        this.querySql = querySql;
        this.queryParams = queryParams;
        this.rowMapper = rowMapper;
        this.duplicateFilter = duplicateFilter;
        this.batchSize = batchSize;
    }
    
    @Override
    public List<MigrationData> produce() {
        if (!hasMore) {
            return null;
        }
        
        try {
            // 构建参数数组（追加 offset 和 limit）
            Object[] params = new Object[2];
            System.arraycopy(queryParams, 0, params, 0, 2);
//            params[queryParams.length] = batchSize;
//            params[queryParams.length + 1] = offset;
            
            // 查询数据
            List<MigrationData> dataList = sourceJdbc.query(querySql, rowMapper, params);
            
            if (dataList == null || dataList.isEmpty()) {
                hasMore = false;
                logger.info("【生产者】数据已耗尽，共处理 {} 批次", batchCount);
                return null;
            }
            
            batchCount++;
            logger.info("【生产者】批次 {}，查询到 {} 条数据，offset={}", 
                    batchCount, dataList.size(), offset);
            
            // 去重过滤
            long beforeFilter = dataList.size();
            List<MigrationData> filteredData = duplicateFilter.filterDuplicates(dataList);
            long afterFilter = filteredData.size();
            long duplicates = beforeFilter - afterFilter;
            
            logger.info("【生产者】批次 {}，去重：{} -> {} (过滤 {} 条)", 
                    batchCount, beforeFilter, afterFilter, duplicates);
            
            offset += batchSize;
            
            // 如果查询的数据少于 batchSize，说明已经是最后一批
            if (dataList.size() < batchSize) {
                hasMore = false;
            }
            
            return filteredData;
            
        } catch (Exception e) {
            logger.error("【生产者】查询失败，offset={}", offset, e);
            hasMore = false;
            return null;
        }
    }
    
    @Override
    public boolean hasMore() {
        return hasMore;
    }
    
    @Override
    public void close() {
        logger.info("【生产者】关闭，共处理 {} 批次，总 offset={}", batchCount, offset);
    }
    
    /**
     * 创建数据库行映射器
     */
    public static RowMapper<MigrationData> createRowMapper(String contentTemplate) {
        return (rs, rowNum) -> {
            MigrationData data = new MigrationData();
            data.setId(rs.getLong("id"));
            data.setBusinessDate(rs.getTimestamp("business_time"));
            data.setDuplicateKey(rs.getString("user_code"));
            data.setContent(String.format(contentTemplate,
                    rs.getString("user_name"),
                    rs.getDouble("amount")));
            return data;
        };
    }
}
