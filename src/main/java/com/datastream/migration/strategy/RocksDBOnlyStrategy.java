package com.datastream.migration.strategy;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 纯 RocksDB 去重策略（无 BloomFilter）
 */
public class RocksDBOnlyStrategy implements DuplicateStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(RocksDBOnlyStrategy.class);
    
    private final RocksDB rocksDB;
    
    public RocksDBOnlyStrategy(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        
        logger.info("✓ 启用 RocksDB 纯存储去重策略");
        logger.info("  - 优势：内存占用低，架构简单，无需初始化 BloomFilter");
    }
    
    @Override
    public boolean isDuplicate(String key) {
        try {
            // 直接查询 RocksDB
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
            // 直接添加到 RocksDB
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            byte[] value = "1".getBytes(StandardCharsets.UTF_8);
            rocksDB.put(keyBytes, value);
            
        } catch (Exception e) {
            logger.error("添加重复键失败，key={}", key, e);
        }
    }
    

    @Override
    public String getStrategyName() {
        return "RocksDB-Only";
    }
}
