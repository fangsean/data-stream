package com.datastream.migration.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 进度管理器 - 管理生产者和消费者的进度
 * 
 * 支持三种状态：
 * 1. pending: 待处理
 * 2. processing: 处理中
 * 3. completed: 已完成
 */
public class ProgressManager {
    
    private static final Logger logger = LoggerFactory.getLogger(ProgressManager.class);
    
    /**
     * 已完成的日期集合（线程安全）
     */
    private final Set<String> completedDates = ConcurrentHashMap.newKeySet();
    
    /**
     * 处理中的日期集合（线程安全）
     */
    private final Set<String> processingDates = ConcurrentHashMap.newKeySet();
    
    /**
     * 检查点文件路径
     */
    private final String checkpointDir;
    
    /**
     * 检查点文件名
     */
    private final String checkpointFile;
    private final String progressFile;
    
    /**
     * 是否启用断点续传
     */
    private final boolean enableCheckpoint;
    
    public ProgressManager(String checkpointDir, String taskName, boolean enableCheckpoint) {
        this.checkpointDir = checkpointDir;
        this.checkpointFile = checkpointDir + File.separator + taskName + ".checkpoint";
        this.progressFile = checkpointDir + File.separator + taskName + ".progress";
        this.enableCheckpoint = enableCheckpoint;
        
        // 创建目录
        try {
            Files.createDirectories(Paths.get(checkpointDir));
        } catch (IOException e) {
            logger.error("创建断点目录失败：{}", checkpointDir, e);
        }
        
        logger.info("进度管理器初始化完成，目录：{}, 启用：{}", checkpointDir, enableCheckpoint);
    }
    
    /**
     * 标记日期为"处理中"状态
     * 
     * @param date 日期 (yyyy-MM-dd)
     */
    public void markProcessing(String date) {
        if (!enableCheckpoint) {
            return;
        }
        
        processingDates.add(date);
        saveProgress();  // 立即保存
        
        logger.debug("标记日期为处理中：{}", date);
    }
    
    /**
     * 标记日期为"已完成"状态
     * 
     * @param date 日期 (yyyy-MM-dd)
     */
    public void markCompleted(String date) {
        if (!enableCheckpoint) {
            return;
        }
        
        processingDates.remove(date);
        completedDates.add(date);
        
        saveProgress();  // 立即保存处理中状态
        saveCheckpoint();  // 保存已完成状态
        
        logger.debug("标记日期为已完成：{}", date);
    }
    
    /**
     * 检查日期是否已完成
     */
    public boolean isCompleted(String date) {
        if (!enableCheckpoint) {
            return false;
        }
        
        return completedDates.contains(date);
    }
    
    /**
     * 检查日期是否正在处理中
     */
    public boolean isProcessing(String date) {
        if (!enableCheckpoint) {
            return false;
        }
        
        return processingDates.contains(date);
    }
    
    /**
     * 获取所有已完成的日期
     */
    public Set<String> getCompletedDates() {
        return this.completedDates;
    }
    
    /**
     * 获取所有处理中的日期（用于恢复）
     */
    public Set<String> getProcessingDates() {
        return this.processingDates;
    }
    
    /**
     * 保存进度到磁盘（处理中的状态）
     */
    private void saveProgress() {
        if (!enableCheckpoint) {
            return;
        }
        
        try {
            // 写入临时文件
            String tempFile = progressFile + ".tmp";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                for (String date : processingDates) {
                    writer.write(date);
                    writer.newLine();
                }
                writer.flush();
            }
            
            // 原子替换正式文件
            Path tempPath = Paths.get(tempFile);
            Path targetPath = Paths.get(progressFile);
            Files.move(tempPath, targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            logger.debug("✓ 进度已保存：{} 个处理中的日期", processingDates.size());
            
        } catch (IOException e) {
            logger.error("保存进度失败", e);
        }
    }
    
    /**
     * 保存断点到磁盘（已完成的状态）
     */
    private void saveCheckpoint() {
        if (!enableCheckpoint) {
            return;
        }
        
        try {
            String tempFile = checkpointFile + ".tmp";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                for (String date : completedDates) {
                    writer.write(date);
                    writer.newLine();
                }
                writer.flush();
            }
            
            Path tempPath = Paths.get(tempFile);
            Path targetPath = Paths.get(checkpointFile);
            Files.move(tempPath, targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            logger.info("✓ 断点已保存：{} 个已完成的日期", completedDates.size());
            
        } catch (IOException e) {
            logger.error("保存断点失败", e);
        }
    }
    
    /**
     * 从磁盘加载进度和断点
     */
    public void load() {
        if (!enableCheckpoint) {
            return;
        }
        
        try {
            // 1. 加载已完成的日期
            Path checkpointPath = Paths.get(checkpointFile);
            if (Files.exists(checkpointPath)) {
                try (BufferedReader reader = Files.newBufferedReader(checkpointPath)) {
                    String line;
                    int count = 0;
                    while ((line = reader.readLine()) != null) {
                        String date = line.trim();
                        if (!date.isEmpty()) {
                            completedDates.add(date);
                            count++;
                        }
                    }
                    logger.info("✓ 已加载 {} 个已完成的日期", count);
                }
            }
            
            // 2. 加载处理中的日期（需要重新处理）
            Path progressPath = Paths.get(progressFile);
            if (Files.exists(progressPath)) {
                try (BufferedReader reader = Files.newBufferedReader(progressPath)) {
                    String line;
                    int count = 0;
                    while ((line = reader.readLine()) != null) {
                        String date = line.trim();
                        if (!date.isEmpty()) {
                            processingDates.add(date);
                            count++;
                        }
                    }
                    logger.warn("⚠ 发现 {} 个未完成的日期（上次服务异常中断），需要重新处理：{}", 
                            count, processingDates);
                }
            }
            
        } catch (IOException e) {
            logger.error("加载进度失败", e);
        }
    }
    
    /**
     * 清空所有进度和断点
     */
    public void clear() {
        completedDates.clear();
        processingDates.clear();
        
        if (enableCheckpoint) {
            try {
                Path checkpointPath = Paths.get(checkpointFile);
                Path progressPath = Paths.get(progressFile);
                
                if (Files.exists(checkpointPath)) {
                    Files.delete(checkpointPath);
                    logger.info("✓ 断点文件已删除：{}", checkpointFile);
                }
                
                if (Files.exists(progressPath)) {
                    Files.delete(progressPath);
                    logger.info("✓ 进度文件已删除：{}", progressFile);
                }
            } catch (IOException e) {
                logger.error("删除文件失败", e);
            }
        }
        
        logger.info("所有进度和断点已清空");
    }
    
    /**
     * 获取已完成的日期数量
     */
    public int getCompletedCount() {
        return completedDates.size();
    }
    
    /**
     * 获取处理中的日期数量
     */
    public int getProcessingCount() {
        return processingDates.size();
    }
}
