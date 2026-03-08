package com.datastream.migration.checkpoint;

import com.datastream.migration.enums.CheckpointType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 断点续传管理器
 * 
 * 功能：
 * 1. 记录每个日期的处理状态
 * 2. 支持多线程并发更新
 * 3. 自动定期落盘
 * 4. 服务关闭时完整保存
 */
public class CheckpointManager {
    
    private static final Logger logger = LoggerFactory.getLogger(CheckpointManager.class);
    
    /**
     * 已完成的日期集合（线程安全）
     * Key: 日期 (yyyy-MM-dd)
     */
    private final Set<String> completedDates = Collections.newSetFromMap(new ConcurrentHashMap<>());
    
    /**
     * 每个日期的预期数据量统计
     * Key: 日期 (yyyy-MM-dd), Value: 预期数量
     */
    private final Map<String, Long> expectedCounts = new ConcurrentHashMap<>();
    
    /**
     * 检查点文件路径
     */
    private final String checkpointDir;
    
    /**
     * 检查点文件名
     */
    private final String checkpointFile;
    
    /**
     * 是否启用断点续传
     */
    private final boolean enableCheckpoint;
    
    public CheckpointManager(String checkpointDir, CheckpointType type, boolean enableCheckpoint) {
        this.checkpointDir = checkpointDir;
        this.checkpointFile = checkpointDir + File.separator + type.getCode() + ".checkpoint";
        this.enableCheckpoint = enableCheckpoint;
        
        // 创建目录
        try {
            Files.createDirectories(Paths.get(checkpointDir));
        } catch (IOException e) {
            logger.error("创建断点目录失败：{}", checkpointDir, e);
        }
        
        logger.info("断点管理器初始化完成，文件：{}, 类型：{}, 启用：{}", 
                this.checkpointFile, type.getDescription(), enableCheckpoint);
    }
    
    /**
     * 标记日期为已完成
     * 
     * @param date 日期 (yyyy-MM-dd)
     */
    public void markCompleted(String date) {
        if (!enableCheckpoint) {
            return;
        }
        
        completedDates.add(date);
        logger.debug("标记日期完成：{}", date);
    }
    
    /**
     * 标记日期为已完成（带数量统计）
     * 
     * @param date 日期 (yyyy-MM-dd)
     * @param count 实际处理的数量
     */
    public void markCompleted(String date, long count) {
        if (!enableCheckpoint) {
            return;
        }
        
        completedDates.add(date);
        expectedCounts.put(date, count);
        logger.info("✓ 标记日期完成：{}, 实际处理 {} 条", date, count);
    }
    
    /**
     * 检查日期是否已完成
     * 
     * @param date 日期 (yyyy-MM-dd)
     * @return true-已完成，false-未完成
     */
    public boolean isCompleted(String date) {
        if (!enableCheckpoint) {
            return false;
        }
        
        return completedDates.contains(date);
    }
    
    /**
     * 记录日期的预期数量（由生产者在生成数据时统计）
     * 
     * @param date 日期
     * @param expectedCount 预期数量
     */
    public void recordExpectedCount(String date, long expectedCount) {
        if (!enableCheckpoint) {
            return;
        }
        
        expectedCounts.put(date, expectedCount);
        logger.debug("记录预期数量：{} - {} 条", date, expectedCount);
    }
    
    /**
     * 获取日期的预期数量
     */
    public Long getExpectedCount(String date) {
        return expectedCounts.get(date);
    }
    
    /**
     * 获取所有已完成的日期
     * 
     * @return 已完成的日期集合
     */
    public Set<String> getCompletedDates() {
        return Collections.unmodifiableSet(completedDates);
    }
    
    /**
     * 标记日期为已完成（带数量统计）
     * 
     * @param date 日期 (yyyy-MM-dd)
     * @param actualInsertCount 实际入库数量
     * @param duplicateCount 重复数据数量
     * @param expectedCount 预期总数量（用于验证）
     * @return true-数量匹配，false-数量不匹配
     */
    public boolean markCompleted(String date, long actualInsertCount, long duplicateCount, long expectedCount) {
        if (!enableCheckpoint) {
            return false;
        }
        
        // 验证公式：生产者天总数 = 消费入库数 + 判断重复数
        long calculatedTotal = actualInsertCount + duplicateCount;
        boolean isValid = (calculatedTotal == expectedCount);
        
        if (isValid) {
            completedDates.add(date);
            expectedCounts.put(date, expectedCount);
            logger.info("✓ 日期 {} 已完成验证：入库 {} 条 + 重复 {} 条 = {} 条 (预期 {} 条) ✓",
                    date, actualInsertCount, duplicateCount, calculatedTotal, expectedCount);
        } else {
            logger.error("✗ 日期 {} 数量验证失败！入库 {} 条 + 重复 {} 条 = {} 条，但预期是 {} 条",
                    date, actualInsertCount, duplicateCount, calculatedTotal, expectedCount);
        }
        
        return isValid;
    }
    
    /**
     * 保存断点到磁盘
     */
    public void save() {
        if (!enableCheckpoint) {
            logger.info("未启用断点续传，跳过保存");
            return;
        }
        
        try {
            // 1. 保存已完成的日期
            String tempFile = checkpointFile + ".tmp";
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                for (String date : completedDates) {
                    writer.write(date);
                    writer.newLine();
                }
                writer.flush();
            }
            
            // 原子替换正式文件
            Path tempPath = Paths.get(tempFile);
            Path targetPath = Paths.get(checkpointFile);
            Files.move(tempPath, targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            logger.info("✓ 断点已保存：{} 个日期，文件：{}", completedDates.size(), checkpointFile);
            
            // 2. 保存统计数据到单独文件
            saveStatistics();
            
        } catch (IOException e) {
            logger.error("保存断点失败", e);
        }
    }
    
    /**
     * 保存统计数据
     */
    private void saveStatistics() {
        try {
            String statsFile = checkpointDir + File.separator + "statistics.dat";
            String tempFile = statsFile + ".tmp";
            
            try (BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile))) {
                for (Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
                    writer.write(entry.getKey());
                    writer.write("=");
                    writer.write(String.valueOf(entry.getValue()));
                    writer.newLine();
                }
                writer.flush();
            }
            
            Path tempPath = Paths.get(tempFile);
            Path targetPath = Paths.get(statsFile);
            Files.move(tempPath, targetPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            
            logger.debug("✓ 统计数据已保存：{} 条记录", expectedCounts.size());
            
        } catch (IOException e) {
            logger.error("保存统计数据失败", e);
        }
    }
    
    /**
     * 从磁盘加载断点
     */
    public void load() {
        if (!enableCheckpoint) {
            logger.info("未启用断点续传，跳过加载");
            return;
        }
        
        try {
            Path path = Paths.get(checkpointFile);
            if (!Files.exists(path)) {
                logger.info("断点文件不存在：{}", checkpointFile);
                return;
            }
            
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    String date = line.trim();
                    if (!date.isEmpty()) {
                        completedDates.add(date);
                        count++;
                    }
                }
                logger.info("✓ 断点已加载：{} 个日期，文件：{}", count, checkpointFile);
            }
            
            // 加载统计数据
            loadStatistics();
            
        } catch (IOException e) {
            logger.error("加载断点失败", e);
        }
    }
    
    /**
     * 加载统计数据
     */
    private void loadStatistics() {
        try {
            String statsFile = checkpointDir + File.separator + "statistics.dat";
            Path path = Paths.get(statsFile);
            
            if (!Files.exists(path)) {
                logger.debug("统计文件不存在：{}", statsFile);
                return;
            }
            
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                String line;
                int count = 0;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("=");
                    if (parts.length == 2) {
                        String date = parts[0].trim();
                        long expectedCount = Long.parseLong(parts[1].trim());
                        expectedCounts.put(date, expectedCount);
                        count++;
                    }
                }
                logger.info("✓ 统计数据已加载：{} 条记录", count);
            }
            
        } catch (IOException e) {
            logger.error("加载统计数据失败", e);
        }
    }
    
    /**
     * 验证并修复断点（用于检测数据一致性）
     * 
     * @param expectedCount 预期的数据量（由外部提供，如生产者统计）
     * @param actualCount 实际的数据量（如 RocksDB 中的记录数）
     * @return true-数据一致，false-数据不一致需要重新处理
     */
    public boolean validateAndFix(long expectedCount, long actualCount) {
        if (!enableCheckpoint) {
            return true;  // 未启用断点续传，不需要验证
        }
        
        if (expectedCount == actualCount) {
            logger.info("✓ 数据一致性校验通过：预期 {}, 实际 {}", expectedCount, actualCount);
            return true;
        } else {
            logger.warn("⚠ 数据一致性校验失败！预期 {}, 实际 {}", expectedCount, actualCount);
            logger.warn("  可能的原因：服务在处理过程中异常中断");
            logger.warn("  建议操作：清空断点，重新处理");
            
            // 可以选择自动清空断点
            // clear();
            
            return false;
        }
    }
    
    /**
     * 清空断点
     */
    public void clear() {
        completedDates.clear();
        
        if (enableCheckpoint) {
            try {
                Path path = Paths.get(checkpointFile);
                if (Files.exists(path)) {
                    Files.delete(path);
                    logger.info("✓ 断点文件已删除：{}", checkpointFile);
                }
            } catch (IOException e) {
                logger.error("删除断点文件失败", e);
            }
        }
        
        logger.info("断点已清空");
    }
    
    /**
     * 获取已完成的日期数量
     * 
     * @return 数量
     */
    public int getCompletedCount() {
        return completedDates.size();
    }
}
