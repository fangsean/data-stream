package com.datastream.migration.strategy;

import com.datastream.migration.filter.DuplicateFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * 目标数据加载策略（B 库已有数据加载到 RocksDB）
 */
public class TargetDataLoadStrategy implements DataLoadStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(TargetDataLoadStrategy.class);
    
    private final JdbcTemplate targetJdbc;
    private final DuplicateFilter filter;
    private final String querySql;
    
    public TargetDataLoadStrategy(JdbcTemplate targetJdbc, DuplicateFilter filter, String querySql) {
        this.targetJdbc = targetJdbc;
        this.filter = filter;
        this.querySql = querySql;
    }
    
    @Override
    public void loadData(String startDate, String endDate) {
        logger.info("开始加载 B 库已有数据到 RocksDB...");
        logger.info("加载日期范围：{} ~ {}", startDate, endDate);
        
        // 调用现有的 TargetDataLoader
        com.datastream.migration.helper.TargetDataLoader loader = 
            new com.datastream.migration.helper.TargetDataLoader();
        loader.loadToRocksDB(targetJdbc, filter, querySql, startDate, endDate);
    }
    
    @Override
    public String getName() {
        return "TargetDataLoader(B 库→RocksDB)";
    }
}
