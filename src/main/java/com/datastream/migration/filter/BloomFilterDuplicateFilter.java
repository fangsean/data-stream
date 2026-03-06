package com.datastream.migration.filter;

import com.datastream.migration.enums.ErrorType;
import com.datastream.migration.model.ErrorRecord;
import com.datastream.migration.model.MigrationConfig;
import com.datastream.migration.model.MigrationData;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于 BloomFilter 和数据文件的去重过滤器实现
 * 特点：
 * 1. 使用 BloomFilter 进行快速去重判断，内存占用小
 * 2. 支持过滤数据持久化到磁盘
 * 3. 支持异常数据追溯
 * 4. 线程安全，支持并发访问
 * 5. 零错误率（BloomFilter 可能存在误判，但通过二次校验确保准确性）
 * 
 * @author DataStream
 * @version 1.0
 */
public class BloomFilterDuplicateFilter implements DuplicateFilter {
    
    private static final Logger logger = LoggerFactory.getLogger(BloomFilterDuplicateFilter.class);
    
    /**
     * 迁移配置
     */
    private final MigrationConfig config;
    
    /**
     * Guava BloomFilter - 用于快速判断是否可能重复
     */
    private BloomFilter<String> bloomFilter;
    
    /**
     * 精确去重 Map - 用于二次校验，确保零错误率
     * Key: duplicateKey, Value: 数据 ID
     */
    private final ConcurrentHashMap<String, Long> exactMatchMap;
    
    /**
     * 过滤数据存储文件路径
     */
    private final String filterDataFile;
    
    /**
     * 异常数据存储文件路径
     */
    private final String errorDataFile;
    
    /**
     * 已处理的数据量统计
     */
    private volatile long processedCount = 0;
    
    /**
     * 过滤的重复数据量统计
     */
    private volatile long filteredCount = 0;
    
    /**
     * 构造函数
     * 
     * @param config 迁移配置
     */
    public BloomFilterDuplicateFilter(MigrationConfig config) {
        this.config = config;
        this.exactMatchMap = new ConcurrentHashMap<>();
        
        // 设置文件路径
        if (config.getFilterDataPath() != null) {
            this.filterDataFile = config.getFilterDataPath() + File.separator + "filter_data.dat";
            this.errorDataFile = config.getFilterDataPath() + File.separator + "error_data.dat";
        } else {
            this.filterDataFile = "filter_data.dat";
            this.errorDataFile = "error_data.dat";
        }
        
        logger.info("BloomFilter 去重过滤器初始化完成，预期数据量：{}, 误判率：{}", 
                config.getExpectedInsertions(), config.getFpp());
    }
    
    /**
     * 初始化过滤器
     * 创建 BloomFilter 并加载历史数据
     */
    @Override
    public void init() {
        try {
            // 创建 BloomFilter
            // 使用字符串漏斗，将 duplicateKey 编码为 UTF-8 字节
            this.bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    config.getExpectedInsertions(),
                    config.getFpp()
            );
            
            logger.info("BloomFilter 创建成功，预计插入元素数：{}", config.getExpectedInsertions());
            
            // 加载历史过滤数据
            loadFromFile();
            
            logger.info("过滤器初始化完成，当前元素数量：{}", size());
        } catch (Exception e) {
            logger.error("过滤器初始化失败", e);
            throw new RuntimeException("过滤器初始化失败", e);
        }
    }
    
    /**
     * 检查数据是否重复
     * 双重校验机制：
     * 1. BloomFilter 快速判断（可能存在误判）
     * 2. ConcurrentHashMap 精确校验（确保零错误率）
     * 
     * @param data 待检查的迁移数据
     * @return true-重复，false-不重复
     */
    @Override
    public boolean isDuplicate(MigrationData data) {
        if (data == null || data.getDuplicateKey() == null) {
            return false;
        }
        
        String duplicateKey = data.getDuplicateKey().trim();
        
        // 第一重：BloomFilter 快速判断
        // 如果 BloomFilter 认为不存在，则一定不存在
        if (!bloomFilter.mightContain(duplicateKey)) {
            return false;
        }
        
        // 第二重：ConcurrentHashMap 精确校验
        // 确保零错误率
        return exactMatchMap.containsKey(duplicateKey);
    }
    
    /**
     * 批量检查数据是否重复
     * 使用并行流提高处理效率
     * 
     * @param dataList 待检查的迁移数据列表
     * @return 过滤后的数据列表（不包含重复数据）
     */
    @Override
    public List<MigrationData> filterDuplicates(List<MigrationData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return new ArrayList<>();
        }
        
        List<MigrationData> filteredList = new ArrayList<>(dataList.size());
        
        // 遍历数据并进行去重
        for (MigrationData data : dataList) {
            try {
                if (!isDuplicate(data)) {
                    // 不重复，添加到结果列表
                    filteredList.add(data);
                    
                    // 同时添加到过滤器
                    add(data);
                    
                    processedCount++;
                } else {
                    // 重复数据，记录异常
                    filteredCount++;
                    recordError(ErrorType.DUPLICATE, data, "数据重复");
                    
                    if (config.isDetailedLogging() && processedCount % 10000 == 0) {
                        logger.info("已处理 {} 条数据，过滤 {} 条重复数据", processedCount, filteredCount);
                    }
                }
            } catch (Exception e) {
                logger.error("过滤数据时发生异常，数据 ID: {}", data.getId(), e);
                recordError(ErrorType.TRANSFORM_ERROR, data, "过滤异常：" + e.getMessage());
            }
        }
        
        logger.info("批量过滤完成，输入：{}, 输出：{}, 过滤：{}", 
                dataList.size(), filteredList.size(), dataList.size() - filteredList.size());
        
        return filteredList;
    }
    
    /**
     * 将数据添加到过滤器中
     * 线程安全
     * 
     * @param data 迁移数据
     */
    @Override
    public void add(MigrationData data) {
        if (data == null || data.getDuplicateKey() == null) {
            return;
        }
        
        String duplicateKey = data.getDuplicateKey().trim();
        
        // 添加到 BloomFilter
        bloomFilter.put(duplicateKey);
        
        // 添加到精确匹配 Map（如果已存在则覆盖）
        exactMatchMap.put(duplicateKey, data.getId());
    }
    
    /**
     * 批量添加数据到过滤器中
     * 
     * @param dataList 迁移数据列表
     */
    @Override
    public void addAll(List<MigrationData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }
        
        for (MigrationData data : dataList) {
            add(data);
        }
        
        logger.info("批量添加数据完成，数量：{}", dataList.size());
    }
    
    /**
     * 保存过滤器数据到磁盘
     * 只保存精确匹配 Map 的数据（ConcurrentHashMap 中的数据）
     * BloomFilter 由于是概率性数据结构，不保存，重启后重新构建
     */
    @Override
    public void saveToFile() {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        
        try {
            // 创建目录（如果不存在）
            File filterDir = new File(config.getFilterDataPath());
            if (!filterDir.exists()) {
                filterDir.mkdirs();
            }
            
            // 保存精确匹配 Map
            fos = new FileOutputStream(filterDataFile);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(exactMatchMap);
            
            logger.info("过滤器数据保存到磁盘成功，文件：{}, 元素数量：{}", filterDataFile, exactMatchMap.size());
        } catch (IOException e) {
            logger.error("保存过滤器数据失败", e);
            throw new RuntimeException("保存过滤器数据失败", e);
        } finally {
            // 关闭资源
            closeResource(oos, fos);
        }
    }
    
    /**
     * 从磁盘加载过滤器数据
     * 恢复之前保存的精确匹配 Map
     */
    @SuppressWarnings("unchecked")
    @Override
    public void loadFromFile() {
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        
        try {
            File file = new File(filterDataFile);
            if (!file.exists()) {
                logger.info("过滤数据文件不存在，跳过加载：{}", filterDataFile);
                return;
            }
            
            // 加载精确匹配 Map
            fis = new FileInputStream(file);
            ois = new ObjectInputStream(fis);
            ConcurrentHashMap<String, Long> loadedMap = 
                    (ConcurrentHashMap<String, Long>) ois.readObject();
            
            // 将加载的数据添加到当前 Map
            exactMatchMap.putAll(loadedMap);
            
            // 重新构建 BloomFilter（优化：分批插入避免 OOM）
            rebuildBloomFilter();
            
            logger.info("从磁盘加载过滤器数据成功，文件：{}, 元素数量：{}", filterDataFile, exactMatchMap.size());
        } catch (FileNotFoundException e) {
            logger.warn("过滤数据文件不存在：{}", filterDataFile);
        } catch (ClassNotFoundException e) {
            logger.error("加载过滤器数据失败：类未找到", e);
            throw new RuntimeException("加载过滤器数据失败", e);
        } catch (IOException e) {
            logger.error("加载过滤器数据失败：IO 异常", e);
            throw new RuntimeException("加载过滤器数据失败", e);
        } finally {
            // 关闭资源
            closeResource(ois, fis);
        }
    }
    
    /**
     * 清空过滤器
     * 释放内存
     */
    @Override
    public void clear() {
        exactMatchMap.clear();
        if (bloomFilter != null) {
            // 重新创建 BloomFilter
            bloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    config.getExpectedInsertions(),
                    config.getFpp()
            );
        }
        processedCount = 0;
        filteredCount = 0;
        logger.info("过滤器已清空");
    }
    
    /**
     * 获取过滤器中的元素数量
     * 
     * @return 元素数量
     */
    @Override
    public long size() {
        return exactMatchMap.size();
    }
    
    /**
     * 销毁过滤器
     * 释放所有资源
     */
    @Override
    public void destroy() {
        try {
            // 先保存数据到磁盘
            saveToFile();
            
            // 清空内存
            clear();
            
            logger.info("过滤器已销毁");
        } catch (Exception e) {
            logger.error("销毁过滤器失败", e);
        }
    }
    
    /**
     * 记录异常数据
     * 用于后续追溯
     * 
     * @param errorType 错误类型
     * @param data 迁移数据
     * @param message 错误信息
     */
    private void recordError(ErrorType errorType, MigrationData data, String message) {
        try {
            ErrorRecord errorRecord = new ErrorRecord();
            errorRecord.setId(generateId());
            errorRecord.setTaskId(config.getTaskId());
            errorRecord.setErrorType(errorType.getCode());
            errorRecord.setOriginalDataId(data.getId());
            errorRecord.setDuplicateKey(data.getDuplicateKey());
            errorRecord.setErrorMessage(message);
            errorRecord.setDataSnapshot(convertToJson(data));
            errorRecord.setOccurTime(new Date());
            errorRecord.setProcessed(false);
            
            // 追加写入到文件
            appendErrorToFile(errorRecord);
        } catch (Exception e) {
            logger.error("记录异常数据失败", e);
        }
    }
    
    /**
     * 将异常记录追加到文件
     * 
     * @param errorRecord 异常记录
     */
    private void appendErrorToFile(ErrorRecord errorRecord) {
        FileOutputStream fos = null;
        ObjectOutputStream oos = null;
        
        try {
            File file = new File(errorDataFile);
            boolean append = file.exists();
            
            fos = new FileOutputStream(file, append);
            oos = new ObjectOutputStream(fos);
            oos.writeObject(errorRecord);
            
            logger.debug("异常记录已追加到文件：{}", errorDataFile);
        } catch (IOException e) {
            logger.error("追加异常记录失败", e);
        } finally {
            closeResource(oos, fos);
        }
    }
    
    /**
     * 重建 BloomFilter
     * 当从磁盘加载数据后，需要重新构建 BloomFilter
     * 采用分批插入策略，避免内存溢出
     */
    private void rebuildBloomFilter() {
        try {
            // 创建新的 BloomFilter
            BloomFilter<String> newBloomFilter = BloomFilter.create(
                    Funnels.stringFunnel(StandardCharsets.UTF_8),
                    exactMatchMap.size(),
                    config.getFpp()
            );
            
            // 分批插入（每 10 万条打印一次进度）
            int count = 0;
            for (String key : exactMatchMap.keySet()) {
                newBloomFilter.put(key);
                count++;
                
                if (count % 100000 == 0) {
                    logger.debug("重建 BloomFilter 进度：{}/{}", count, exactMatchMap.size());
                }
            }
            
            // 替换旧的 BloomFilter
            this.bloomFilter = newBloomFilter;
            
            logger.info("BloomFilter 重建完成，元素数量：{}", count);
        } catch (Exception e) {
            logger.error("重建 BloomFilter 失败", e);
            throw new RuntimeException("重建 BloomFilter 失败", e);
        }
    }
    
    /**
     * 生成唯一 ID
     * 
     * @return 唯一 ID
     */
    private Long generateId() {
        return Math.abs(UUID.randomUUID().getLeastSignificantBits());
    }
    
    /**
     * 将对象转换为 JSON 字符串
     * 
     * @param obj 对象
     * @return JSON 字符串
     */
    private String convertToJson(Object obj) {
        try {
            // 简单实现，实际项目中可使用 Jackson 或 Gson
            if (obj instanceof MigrationData) {
                MigrationData data = (MigrationData) obj;
                return String.format("{\"id\":%d,\"duplicateKey\":\"%s\",\"businessDate\":\"%s\"}",
                        data.getId() != null ? data.getId() : 0,
                        data.getDuplicateKey() != null ? data.getDuplicateKey() : "",
                        data.getBusinessDate() != null ? data.getBusinessDate().toString() : "");
            }
            return obj.toString();
        } catch (Exception e) {
            logger.error("转换为 JSON 失败", e);
            return obj.toString();
        }
    }
    
    /**
     * 关闭资源
     * 
     * @param closeables 可关闭的资源
     */
    private void closeResource(Closeable... closeables) {
        for (Closeable closeable : closeables) {
            if (closeable != null) {
                try {
                    closeable.close();
                } catch (IOException e) {
                    logger.warn("关闭资源失败", e);
                }
            }
        }
    }
}
