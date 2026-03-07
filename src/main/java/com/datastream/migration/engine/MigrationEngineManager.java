package com.datastream.migration.engine;

import com.datastream.migration.consumer.DataConsumer;
import com.datastream.migration.filter.DuplicateFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 迁移引擎管理器（Spring Bean）
 * 
 * 生命周期：
 * - Spring 启动时创建
 * - Spring 停止时销毁
 * 
 * @author DataStream
 */
@Component
public class MigrationEngineManager {
    
    private static final Logger logger = LoggerFactory.getLogger(MigrationEngineManager.class);
    
    @Autowired(required = false)
    private MigrationEngine migrationEngine;
    
    private ScheduledExecutorService statsScheduler;
    private volatile boolean running = false;
    
    /**
     * 启动统计定时器
     */
    public void startStatsReporter() {
        if (migrationEngine == null) {
            logger.warn("迁移引擎未初始化，跳过统计定时器");
            return;
        }
        
        logger.info("启动统计定时器，每 5 秒输出一次进度...");
        
        statsScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = new Thread(r, "Migration-Stats-Reporter");
            thread.setDaemon(true);  // 守护线程
            return thread;
        });
        
        statsScheduler.scheduleAtFixedRate(() -> {
            try {
                reportStats();
            } catch (Exception e) {
                logger.error("统计任务执行失败", e);
            }
        }, 0, 5, TimeUnit.SECONDS);
        
        running = true;
        logger.info("✓ 统计定时器已启动");
    }
    
    /**
     * 报告统计信息
     */
    private void reportStats() {
        if (migrationEngine == null) {
            return;
        }
        
        AtomicLong producedCount = migrationEngine.getProducedCount();
        AtomicLong consumedCount = migrationEngine.getConsumedCount();
        DuplicateFilter filter = migrationEngine.getDuplicateFilter();
        
        long produced = producedCount.get();
        long consumed = consumedCount.get();
        long duplicates = 0;
        
        if (filter != null && filter instanceof com.datastream.migration.filter.RocksDBDuplicateFilter) {
            duplicates = ((com.datastream.migration.filter.RocksDBDuplicateFilter) filter).getDuplicateCount();
        }
        
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          实时统计 ({}s)                    ║", System.currentTimeMillis() % 1000);
        logger.info("╠══════════════════════════════════════════════════════╣");
        logger.info("║ 生产：{} 条", produced);
        logger.info("║ 消费：{} 条", consumed);
        logger.info("║ 去重：{} 条", duplicates);
        logger.info("║ 剩余：{} 条", produced - consumed);
        
        if (produced > 0 && produced == consumed) {
            logger.info("║ 状态：✓ 已完成                              ║");
        } else if (produced > 0) {
            double progress = (consumed * 100.0) / produced;
            logger.info("║ 进度：{}%                            ║", progress);
        } else {
            logger.info("║ 状态：等待数据...                          ║");
        }
        
        logger.info("╚══════════════════════════════════════════════════════╝");
    }
    
    /**
     * 停止统计定时器
     */
    public void stopStatsReporter() {
        if (statsScheduler != null && !statsScheduler.isShutdown()) {
            statsScheduler.shutdown();
            try {
                if (!statsScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    statsScheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                statsScheduler.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("✓ 统计定时器已停止");
        }
        running = false;
    }
    
    /**
     * Spring 容器销毁前调用
     */
    @PreDestroy
    public void destroy() {
        logger.info("╔══════════════════════════════════════════════════════╗");
        logger.info("║          Spring 容器关闭，清理资源                   ║");
        logger.info("╚══════════════════════════════════════════════════════╝");
        
        // 1. 停止统计定时器
        stopStatsReporter();
        
        // 2. 停止迁移引擎
        if (migrationEngine != null) {
            migrationEngine.stop();
            logger.info("✓ 迁移引擎已停止");
        }
        
        // 3. 销毁过滤器
        if (migrationEngine != null && migrationEngine.getDuplicateFilter() != null) {
            try {
                migrationEngine.getDuplicateFilter().saveToFile();
                migrationEngine.getDuplicateFilter().close();
                logger.info("✓ RocksDB 过滤器已保存并关闭");
            } catch (Exception e) {
                logger.error("✗ 销毁过滤器失败", e);
            }
        }
        
        logger.info("✓ 所有资源已清理完成");
    }
    
    public boolean isRunning() {
        return running;
    }
}
