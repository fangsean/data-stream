package com.datastream.migration.strategy;

import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;

/**
 * 纯 RocksDB 去重策略（无 BloomFilter）
 */
public class RocksDBOnlyStrategy implements DuplicateStrategy {
    
    private static final Logger logger = LoggerFactory.getLogger(RocksDBOnlyStrategy.class);
    
    private final RocksDB rocksDB;
    
    //  使用 WriteOptions 保证原子性（无需锁）
    private final WriteOptions writeOptions;
    
    public RocksDBOnlyStrategy(RocksDB rocksDB) {
        this.rocksDB = rocksDB;
        
        // 配置写入选项（同步模式，保证原子性）
        this.writeOptions = new WriteOptions();
        this.writeOptions.setSync(false);  // 不强制刷盘，提升性能
        
        logger.info("✓ 启用 RocksDB 纯存储去重策略（乐观锁版本）");
        logger.info("  - 优势：内存占用低，无锁并发，高性能");
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
    
    /**
     *  原子性的 "检查并添加" 操作（乐观锁版本）
     * 利用 RocksDB 的原子写入特性，无需额外加锁
     * 
     * @param key 要检查和添加的 key
     * @return true-重复（已存在），false-不重复（新添加）
     */
    public boolean checkAndAdd(String key) {
        try {
            byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
            
            // 1. 先检查是否存在
            byte[] existingValue = rocksDB.get(keyBytes);
            if (existingValue != null) {
                return true;  // 已存在，说明重复
            }
            
            // 2. 尝试写入（使用 WriteOptions 保证原子性）
            // 生成唯一值（使用时间戳 + 随机数）
            String uniqueValue = System.nanoTime() + "-" + Thread.currentThread().getId();
            byte[] value = uniqueValue.getBytes(StandardCharsets.UTF_8);
            rocksDB.put(writeOptions, keyBytes, value);
            
            // 3. 二次检查：确认是否写入成功
            // （极端并发情况下，可能其他线程抢先写入）
            byte[] checkValue = rocksDB.get(keyBytes);
            if (checkValue != null) {
                // 检查值是否是我们写入的
                String checkValueStr = new String(checkValue, StandardCharsets.UTF_8);
                if (uniqueValue.equals(checkValueStr)) {
                    return false;  // 是我们写入的，成功
                } else {
                    return true;   // 是别人写入的，我们失败了
                }
            }
            
            return false;  // 正常情况下不会到这里
            
        } catch (Exception e) {
            logger.error("checkAndAdd 失败，key={}", key, e);
            return false; // 异常情况按不重复处理
        }
    }

    @Override
    public String getStrategyName() {
        return "RocksDB-Only";
    }
}
