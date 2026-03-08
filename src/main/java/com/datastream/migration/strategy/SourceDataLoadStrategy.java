package com.datastream.migration.strategy;

import com.datastream.migration.checkpoint.CheckpointManager;
import com.datastream.migration.enums.CheckpointType;
import com.datastream.migration.filter.RocksDBDuplicateFilter;
import com.datastream.migration.producer.DatabaseQueryProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 源数据加载策略（A 库查询生产者）
 */
public class SourceDataLoadStrategy implements DataLoadStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(SourceDataLoadStrategy.class);
    
    private final JdbcTemplate sourceJdbc;
    private final String querySql;
    private final RocksDBDuplicateFilter filter;
    private final List<DatabaseQueryProducer> producers;
    
    public SourceDataLoadStrategy(JdbcTemplate sourceJdbc, String querySql, 
                                  RocksDBDuplicateFilter filter) {
        this.sourceJdbc = sourceJdbc;
        this.querySql = querySql;
        this.filter = filter;
        this.producers = new ArrayList<>();
    }
    
    @Override
    public void loadData(String startDate, String endDate) {
        // 使用默认配置（启用断点续传）
        loadDataWithCheckpoint(startDate, endDate, true);
    }
    
    /**
     * 加载数据（支持断点续传）
     */
    public void loadDataWithCheckpoint(String startDate, String endDate, boolean enableCheckpoint) {
        logger.info("开始创建 A 库查询生产者...");
        logger.info("加载日期范围：{} ~ {}", startDate, endDate);
        logger.info("断点续传：{}", enableCheckpoint ? "已启用" : "未启用");
        
        // 1. 创建断点管理器（只读模式）
        CheckpointManager checkpointManager = new CheckpointManager(
            "data/checkpoint",
            CheckpointType.SOURCE_DATA_MIGRATE,
            enableCheckpoint
        );
        
        try {
            // 2. 加载断点
            if (enableCheckpoint) {
                checkpointManager.load();
            }
            
            // 3. 生成待处理日期列表（跳过已完成的）
            List<java.time.LocalDate> dateList = generateDatesToCheck(startDate, endDate, enableCheckpoint, checkpointManager);
            
            // 4. 为每天创建生产者
            for (int i = 0; i < dateList.size(); i++) {
                java.time.LocalDate businessDate = dateList.get(i);
                DatabaseQueryProducer producer = createProducerForDate(
                    sourceJdbc, businessDate, i, filter
                );
                producers.add(producer);
                logger.info("✓ 创建生产者 {}，负责日期：{} (时间范围：{} 00:00:00 ~ 23:59:59)", 
                    i, businessDate, businessDate);
                
                // 5. ⚠️ 注意：生产端不标记完成，只读取断点跳过已完成的日期
                // 真正的完成标记由消费端在数据成功入库后执行
            }
            
            logger.info("✓ 生产者创建完成，总数：{}", producers.size());
            
        } catch (Exception e) {
            logger.error("✗ 生产者创建失败", e);
            throw new RuntimeException("生产者创建失败", e);
        }
        // ⚠️ 注意：生产端不需要保存断点，也不需要在 finally 中保存
    }
    
    /**
     * 获取所有生产者
     */
    public List<DatabaseQueryProducer> getProducers() {
        return new ArrayList<>(producers);
    }
    
    /**
     * 生成待处理日期列表（跳过已完成的）
     */
    private List<java.time.LocalDate> generateDatesToCheck(String startDate, String endDate, 
                                                            boolean enableCheckpoint, 
                                                            CheckpointManager checkpointManager) {
        List<java.time.LocalDate> list = new ArrayList<>();
        java.time.LocalDate startLocalDate = java.time.LocalDate.parse(startDate);
        java.time.LocalDate endLocalDate = java.time.LocalDate.parse(endDate);
        
        java.time.LocalDate currentDate = startLocalDate;
        int skippedCount = 0;
        
        while (!currentDate.isAfter(endLocalDate)) {
            String dateStr = currentDate.toString();
            
            // 如果启用断点续传且该日期已完成，则跳过
            if (enableCheckpoint && checkpointManager.isCompleted(dateStr)) {
                logger.info("⊘ 跳过已完成的日期：{}", dateStr);
                skippedCount++;
            } else {
                list.add(currentDate);
            }
            
            currentDate = currentDate.plusDays(1);
        }
        
        logger.info("生成日期列表：从 {} 到 {}，共 {} 天，跳过 {} 天", 
            startDate, endDate, list.size(), skippedCount);
        return list;
    }
    
    /**
     * 生成日期列表（旧方法，保留兼容性）
     */
    @Deprecated
    private List<java.time.LocalDate> generateDateList(String startDate, String endDate) {
        List<java.time.LocalDate> list = new ArrayList<>();
        java.time.LocalDate startLocalDate = java.time.LocalDate.parse(startDate);
        java.time.LocalDate endLocalDate = java.time.LocalDate.parse(endDate);
        
        java.time.LocalDate currentDate = startLocalDate;
        while (!currentDate.isAfter(endLocalDate)) {
            list.add(currentDate);
            currentDate = currentDate.plusDays(1);
        }
        
        logger.info("生成日期列表：从 {} 到 {}，共 {} 天", 
            startDate, endDate, list.size());
        return list;
    }
    
    /**
     * 创建单天生产者
     */
    private DatabaseQueryProducer createProducerForDate(JdbcTemplate sourceJdbc, java.time.LocalDate date, 
                                                         int index, RocksDBDuplicateFilter filter) {
        // 构建当天的时间区间参数
        String startTime = date.atStartOfDay().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String endTime = date.atTime(23, 59, 59).format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        
        Object[] params = new Object[]{
            startTime,   // 当天开始时间
            endTime,     // 当天结束时间
            5000,        // limit
            0            // offset
        };
        
        logger.debug("创建生产者 {} - 日期：{}, 时间范围：{} ~ {}", 
                index, date, startTime, endTime);
        
        return new DatabaseQueryProducer(
            sourceJdbc,
            querySql,
            params,
            DatabaseQueryProducer.createRowMapper("{\"user_name\":\"%s\",\"amount\":%.2f}"),
            filter,
            5000
        );
    }
    
    @Override
    public String getName() {
        return "SourceDataLoader(A 库→生产者)";
    }
}
