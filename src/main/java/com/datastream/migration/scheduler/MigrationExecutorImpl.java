package com.datastream.migration.scheduler;

import com.datastream.migration.filter.DuplicateFilter;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import com.datastream.migration.model.MigrationResult;
import com.datastream.migration.service.DataProcessService;
import com.datastream.migration.service.DataSourceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 迁移任务执行器实现类
 * 特点：
 * 1. 支持按日期分批查询和处理数据
 * 2. 支持并发处理，提高迁移效率
 * 3. 支持任务暂停、恢复、停止
 * 4. 实时统计迁移进度和结果
 * 5. 异常数据自动记录和追溯
 * 
 * @author DataStream
 * @version 1.0
 */
public class MigrationExecutorImpl implements MigrationExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationExecutorImpl.class);
    
    /**
     * 迁移配置
     */
    private MigrationConfig config;
    
    /**
     * 数据源服务
     */
    private DataSourceService dataSourceService;
    
    /**
     * 数据处理服务
     */
    private DataProcessService dataProcessService;
    
    /**
     * 去重过滤器
     */
    private DuplicateFilter duplicateFilter;
    
    /**
     * 线程池 - 用于并发处理
     */
    private ExecutorService executorService;
    
    /**
     * 运行状态标志
     */
    private final AtomicBoolean running = new AtomicBoolean(false);
    
    /**
     * 暂停状态标志
     */
    private final AtomicBoolean paused = new AtomicBoolean(false);
    
    /**
     * 已处理记录数
     */
    private final AtomicLong processedCount = new AtomicLong(0);
    
    /**
     * 总记录数
     */
    private long totalCount = 0;
    
    /**
     * 成功记录数
     */
    private final AtomicLong successCount = new AtomicLong(0);
    
    /**
     * 过滤记录数
     */
    private final AtomicLong filteredCount = new AtomicLong(0);
    
    /**
     * 失败记录数
     */
    private final AtomicLong failedCount = new AtomicLong(0);
    
    /**
     * 日期格式化工具
     */
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    
    /**
     * 对象锁 - 用于暂停/恢复控制
     */
    private final Object lock = new Object();
    
    /**
     * 初始化迁移执行器
     * 
     * @param config 迁移配置
     * @param dataSourceService 数据源服务
     * @param dataProcessService 数据处理服务
     * @param duplicateFilter 去重过滤器
     */
    @Override
    public void init(MigrationConfig config, DataSourceService dataSourceService,
                     DataProcessService dataProcessService, DuplicateFilter duplicateFilter) {
        this.config = config;
        this.dataSourceService = dataSourceService;
        this.dataProcessService = dataProcessService;
        this.duplicateFilter = duplicateFilter;
        
        // 初始化线程池
        this.executorService = Executors.newFixedThreadPool(config.getThreadCount());
        
        // 初始化过滤器
        this.duplicateFilter.init();
        
        logger.info("迁移执行器初始化完成，线程数：{}", config.getThreadCount());
    }
    
    /**
     * 执行迁移任务
     * 默认按配置的日期范围执行
     * 
     * @return 迁移结果
     */
    @Override
    public MigrationResult execute() {
        return executeByDateRange(config.getStartDate(), config.getEndDate());
    }
    
    /**
     * 按日期分批执行迁移
     * 核心方法：
     * 1. 按日期分批查询数据
     * 2. 使用 BloomFilter 去重
     * 3. 数据转换和验证
     * 4. 批量入库
     * 5. 记录异常和进度
     * 
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @return 迁移结果
     */
    @Override
    public MigrationResult executeByDateRange(Date startDate, Date endDate) {
        logger.info("开始执行迁移任务，task={}, 日期范围：{} 到 {}", 
                config.getTaskId(), formatDate(startDate), formatDate(endDate));
        
        MigrationResult.MigrationResultBuilder resultBuilder = MigrationResult.builder();
        resultBuilder.taskId(config.getTaskId());
        resultBuilder.startTime(new Date());
        
        try {
            // 设置运行状态
            running.set(true);
            paused.set(false);
            
            // 获取总数据量
            totalCount = dataSourceService.totalCount();
            logger.info("迁移任务总数据量：{}", totalCount);
            
            // 重置计数器
            resetCounters();
            
            // 生成日期列表
            List<Date> dateList = generateDateList(startDate, endDate);
            logger.info("需要迁移的日期数：{}", dateList.size());
            
            // 按日期逐个处理
            for (Date businessDate : dateList) {
                if (!running.get()) {
                    logger.info("迁移任务已停止");
                    break;
                }
                
                // 检查暂停状态
                checkPause();
                
                // 处理指定日期的数据
                processDate(businessDate);
            }
            
            // 保存过滤器数据到磁盘
            duplicateFilter.saveToFile();
            
            // 构建成功结果
            resultBuilder.success(true);
            resultBuilder.detailInfo("迁移任务执行成功");
            
        } catch (Exception e) {
            logger.error("迁移任务执行失败", e);
            resultBuilder.success(false);
            resultBuilder.errorMessage("迁移任务执行失败：" + e.getMessage());
        } finally {
            // 清理资源
            cleanup();
        }
        
        // 设置结束时间和统计信息
        resultBuilder.endTime(new Date());
        resultBuilder.totalRecords(processedCount.get());
        resultBuilder.successCount(successCount.get());
        resultBuilder.filteredCount(filteredCount.get());
        resultBuilder.failedCount(failedCount.get());
        resultBuilder.durationMs(System.currentTimeMillis() - resultBuilder.build().getStartTime().getTime());
        
        MigrationResult result = resultBuilder.build();
        logResult(result);
        
        return result;
    }
    
    /**
     * 暂停迁移任务
     */
    @Override
    public void pause() {
        if (running.get()) {
            paused.set(true);
            logger.info("迁移任务已暂停");
        }
    }
    
    /**
     * 恢复迁移任务
     */
    @Override
    public void resume() {
        if (paused.get()) {
            paused.set(false);
            synchronized (lock) {
                lock.notifyAll();
            }
            logger.info("迁移任务已恢复");
        }
    }
    
    /**
     * 停止迁移任务
     */
    @Override
    public void stop() {
        running.set(false);
        paused.set(false);
        synchronized (lock) {
            lock.notifyAll();
        }
        logger.info("迁移任务已停止");
    }
    
    /**
     * 获取当前进度（百分比）
     * 
     * @return 进度百分比
     */
    @Override
    public double getProgress() {
        if (totalCount == 0) {
            return 0.0;
        }
        return (processedCount.get() * 100.0) / totalCount;
    }
    
    /**
     * 获取已处理的记录数
     * 
     * @return 已处理记录数
     */
    @Override
    public long getProcessedCount() {
        return processedCount.get();
    }
    
    /**
     * 处理指定日期的数据
     * 
     * @param businessDate 业务日期
     */
    private void processDate(Date businessDate) {
        logger.info("开始处理日期：{}", formatDate(businessDate));
        
        try {
            // 计算该日期的数据总量
            long dateTotalCount = dataSourceService.countByBusinessDate(businessDate);
            logger.info("日期 {} 的数据总量：{}", formatDate(businessDate), dateTotalCount);
            
            if (dateTotalCount == 0) {
                logger.info("日期 {} 没有数据", formatDate(businessDate));
                return;
            }
            
            // 分批查询和处理
            int offset = 0;
            int batchSize = config.getBatchSize();
            
            while (offset < dateTotalCount && running.get()) {
                // 检查暂停状态
                checkPause();
                
                // 查询一批数据
                List<MigrationData> dataList = dataSourceService.queryByBusinessDate(
                        businessDate, offset, batchSize);
                
                if (dataList == null || dataList.isEmpty()) {
                    logger.info("日期 {} 的数据已处理完毕", formatDate(businessDate));
                    break;
                }
                
                // 去重过滤
                List<MigrationData> filteredData = duplicateFilter.filterDuplicates(dataList);
                
                // 更新过滤计数
                filteredCount.addAndGet(dataList.size() - filteredData.size());
                
                // 数据处理（转换、验证）
                List<MigrationData> processedData = dataProcessService.processBatch(filteredData);
                
                // 批量入库
                if (!processedData.isEmpty()) {
                    int saveSuccessCount = dataProcessService.saveBatchToTarget(processedData);
                    successCount.addAndGet(saveSuccessCount);
                    failedCount.addAndGet(processedData.size() - saveSuccessCount);
                }
                
                // 更新已处理计数
                processedCount.addAndGet(dataList.size());
                
                // 打印进度
                offset += batchSize;
                if (offset % (batchSize * 10) == 0) {
                    logger.info("日期 {} 处理进度：{}/{}, 百分比：{}%", 
                            formatDate(businessDate), offset, dateTotalCount, 
                            (offset * 100.0) / dateTotalCount);
                }
            }
            
            logger.info("日期 {} 处理完成", formatDate(businessDate));
            
        } catch (Exception e) {
            logger.error("处理日期 {} 失败", formatDate(businessDate), e);
            throw new RuntimeException("处理日期失败", e);
        }
    }
    
    /**
     * 检查暂停状态
     * 如果暂停，则阻塞等待
     */
    private void checkPause() {
        if (paused.get()) {
            synchronized (lock) {
                try {
                    logger.info("任务暂停，等待恢复...");
                    lock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("等待恢复时被中断");
                }
            }
        }
    }
    
    /**
     * 生成日期列表
     * 
     * @param startDate 开始日期
     * @param endDate 结束日期
     * @return 日期列表
     */
    private List<Date> generateDateList(Date startDate, Date endDate) {
        List<Date> dateList = new ArrayList<>();
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(startDate);
        
        while (!calendar.getTime().after(endDate)) {
            dateList.add(calendar.getTime());
            calendar.add(Calendar.DAY_OF_YEAR, 1);
        }
        
        return dateList;
    }
    
    /**
     * 重置计数器
     */
    private void resetCounters() {
        processedCount.set(0);
        successCount.set(0);
        filteredCount.set(0);
        failedCount.set(0);
    }
    
    /**
     * 清理资源
     */
    private void cleanup() {
        running.set(false);
        
        // 关闭线程池
        if (executorService != null) {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
        
        // 关闭服务
        if (dataSourceService != null) {
            dataSourceService.close();
        }
        
        if (dataProcessService != null) {
            dataProcessService.close();
        }
        
        // 销毁过滤器
        if (duplicateFilter != null) {
            duplicateFilter.destroy();
        }
        
        logger.info("迁移任务资源已清理");
    }
    
    /**
     * 打印迁移结果
     * 
     * @param result 迁移结果
     */
    private void logResult(MigrationResult result) {
        logger.info("==================== 迁移任务执行结果 ====================");
        logger.info("任务 ID: {}", result.getTaskId());
        logger.info("是否成功：{}", result.isSuccess());
        logger.info("总记录数：{}", result.getTotalRecords());
        logger.info("成功入库：{}", result.getSuccessCount());
        logger.info("过滤重复：{}", result.getFilteredCount());
        logger.info("失败数量：{}", result.getFailedCount());
        logger.info("耗时 (ms): {}", result.getDurationMs());
        logger.info("开始时间：{}", formatDate(result.getStartTime()));
        logger.info("结束时间：{}", formatDate(result.getEndTime()));
        
        if (result.getErrorMessage() != null) {
            logger.error("错误信息：{}", result.getErrorMessage());
        }
        
        logger.info("==========================================================");
    }
    
    /**
     * 格式化日期
     * 
     * @param date 日期
     * @return 格式化后的字符串
     */
    private String formatDate(Date date) {
        if (date == null) {
            return "null";
        }
        return DATE_FORMAT.format(date);
    }
}
