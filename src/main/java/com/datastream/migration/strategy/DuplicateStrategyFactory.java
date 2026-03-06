package com.datastream.migration.strategy;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 去重策略工厂
 */
public class DuplicateStrategyFactory {
    
    private static final Logger logger = LoggerFactory.getLogger(DuplicateStrategyFactory.class);
    
    /**
     * 根据配置创建去重策略
     * 
     * @param rocksDB RocksDB 实例
     * @param enableBloomFilter 是否启用 BloomFilter
     * @param expectedInsertions 预计数据量
     * @param fpp 误判率
     * @return 去重策略实例
     */
    public static DuplicateStrategy createStrategy(RocksDB rocksDB, 
                                                    boolean enableBloomFilter,
                                                    long expectedInsertions,
                                                    double fpp) {
        
        if (enableBloomFilter) {
            logger.info("╔══════════════════════════════════════════════════════╗");
            logger.info("║          使用 BloomFilter+RocksDB 双重去重策略           ║");
            logger.info("╠══════════════════════════════════════════════════════╣");
            logger.info("║ 优势：查询速度快（O(1) 时间复杂度）                   ║");
            logger.info("║ 缺点：内存占用大，每次启动需初始化 BloomFilter       ║");
            logger.info("╚══════════════════════════════════════════════════════╝");
            
            return new BloomFilterRocksDBStrategy(rocksDB, expectedInsertions, fpp);
            
        } else {
            logger.info("╔══════════════════════════════════════════════════════╗");
            logger.info("║          使用 RocksDB 纯存储去重策略                     ║");
            logger.info("╠══════════════════════════════════════════════════════╣");
            logger.info("║ 优势：内存占用低，架构简单，无需初始化               ║");
            logger.info("║ 缺点：单次查询略慢（但 RocksDB 已经很快）             ║");
            logger.info("╚══════════════════════════════════════════════════════╝");
            
            return new RocksDBOnlyStrategy(rocksDB);
        }
    }
}
