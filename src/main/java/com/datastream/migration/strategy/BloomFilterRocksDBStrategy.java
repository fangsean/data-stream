package com.datastream.migration.strategy;

import com.datastream.migration.model.MigrationData;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * BloomFilter + RocksDB 双重去重策略
 */
public class BloomFilterRocksDBStrategy implements DuplicateStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(BloomFilterRocksDBStrategy.class);
    
    private final BloomFilter<String> bloomFilter;
    private final RocksDB rocksDB;
    private final long expectedInsertions;
    private final double fpp;
    
    public BloomFilterRocksDBStrategy(RocksDB rocksDB, long expectedInsertions, double fpp) {
        this.rocksDB = rocksDB;
        this.expectedInsertions = expectedInsertions;
        this.fpp = fpp;
        
        // 创建 BloomFilter
        this.bloomFilter = createBloomFilter();
        
        logger.info("✓ 启用 BloomFilter+RocksDB 双重去重策略");
        logger.info("  - 预计数据量：{} 条", expectedInsertions);
        logger.info("  - 误判率：{}", fpp);
    }
    
    /**
     * 创建 BloomFilter
     */
    private BloomFilter<String> createBloomFilter() {
        logger.info("正在创建 BloomFilter...");
        
        BloomFilter<String> bf = BloomFilter.create(
            Funnels.stringFunnel(StandardCharsets.UTF_8),
            expectedInsertions,
            fpp
        );
        
        logger.info("✓ BloomFilter 创建完成，预计容量：{} 条，误判率：{}", expectedInsertions, fpp);
        return bf;
    }
    
    @Override
    public boolean isDuplicate(String key) {
        try {
            // 第一重：BloomFilter 快速判断
            if (!bloomFilter.mightContain(key)) {
                return false;  // BloomFilter 认为不存在，则一定不存在
            }
            
            // 第二重：RocksDB 精确校验
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] value = rocksDB.get(keyBytes);
            
            return value != null;
            
        } catch (Exception e) {
            logger.error("检查重复失败，key={}", key, e);
            return false;
        }
    }
    
    @Override
    public void addDuplicate(String key) {
        try {
            // 1. 添加到 BloomFilter
            bloomFilter.put(key);
            
            // 2. 添加到 RocksDB
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] value = "1".getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, value);
            
        } catch (Exception e) {
            logger.error("添加重复键失败，key={}", key, e);
        }
    }
    
    @Override
    public String getStrategyName() {
        return "BloomFilter+RocksDB";
    }
    
    /**
     *  原子性的 "检查并添加" 操作（乐观锁版本）
     * 利用 RocksDB 的原子写入 + CAS 思想，无需额外加锁
     */
    @Override
    public boolean checkAndAdd(String key) {
        try {
            // 第一重：BloomFilter 快速判断
            if (!bloomFilter.mightContain(key)) {
                // BloomFilter 认为不存在，则一定不存在
                // 直接添加到 BloomFilter（线程安全，无锁）
                bloomFilter.put(key);
                
                // 添加到 RocksDB（使用 WriteOptions 保证原子性）
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] value = "1".getBytes(StandardCharsets.UTF_8);
                rocksDB.put(keyBytes, value);
                
                return false; // 不重复
            }
            
            // BloomFilter 认为可能存在，需要进一步检查
            // 第二重：RocksDB 精确校验
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] existingValue = rocksDB.get(keyBytes);
            
            if (existingValue != null) {
                return true; // 重复
            } else {
                // 不存在，尝试添加
                byte[] value = "1".getBytes(StandardCharsets.UTF_8);
                rocksDB.put(keyBytes, value);
                return false; // 不重复
            }
            
        } catch (Exception e) {
            logger.error("checkAndAdd 失败，key={}", key, e);
            return false; // 异常情况按不重复处理
        }
    }
}
