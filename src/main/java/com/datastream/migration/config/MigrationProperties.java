package com.datastream.migration.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 迁移任务配置属性
 */
@Component
@ConfigurationProperties(prefix = "migration")
public class MigrationProperties {

    private int batchSize = 1000;
    private int threadCount = 2;
    private boolean enableBloomFilter = true;
    private boolean enableCheckpoint = true;  // 是否启用断点续传
    private long expectedInsertions = 10000;
    private double fpp = 0.001;
    private String filterDataPath = "data/spring-boot-auto-run";
    private String checkDataPath = "data/checkpoint";

    private TargetDataLoad targetDataLoad = new TargetDataLoad();

    private SourceDataMigrate sourceDataMigrate = new SourceDataMigrate();

    private TargetDataCheckExisting targetDataCheckExisting = new TargetDataCheckExisting();

    private Sql sql = new Sql();

    /**
     * B 库已有数据加载配置
     */
    public static class TargetDataLoad {
        private String startDate;  // 开始日期 yyyy-MM-dd
        private String endDate;    // 结束日期 yyyy-MM-dd

        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }
    }

    /**
     * SQL 配置
     */
    public static class Sql {
        private String query;
        private String insert;
        private String insertError;
        private String checkExisting;

        public String getQuery() {
            return query;
        }

        public void setQuery(String query) {
            this.query = query;
        }

        public String getInsert() {
            return insert;
        }

        public void setInsert(String insert) {
            this.insert = insert;
        }

        public String getInsertError() {
            return insertError;
        }

        public void setInsertError(String insertError) {
            this.insertError = insertError;
        }

        public String getCheckExisting() {
            return checkExisting;
        }

        public void setCheckExisting(String checkExisting) {
            this.checkExisting = checkExisting;
        }
    }

    // Getters and Setters

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }

    public boolean isEnableBloomFilter() {
        return enableBloomFilter;
    }
        
    public void setEnableBloomFilter(boolean enableBloomFilter) {
        this.enableBloomFilter = enableBloomFilter;
    }
        
    public boolean isEnableCheckpoint() {
        return enableCheckpoint;
    }
        
    public void setEnableCheckpoint(boolean enableCheckpoint) {
        this.enableCheckpoint = enableCheckpoint;
    }

    public long getExpectedInsertions() {
        return expectedInsertions;
    }

    public void setExpectedInsertions(long expectedInsertions) {
        this.expectedInsertions = expectedInsertions;
    }

    public double getFpp() {
        return fpp;
    }

    public void setFpp(double fpp) {
        this.fpp = fpp;
    }

    public String getFilterDataPath() {
        return filterDataPath;
    }

    public void setFilterDataPath(String filterDataPath) {
        this.filterDataPath = filterDataPath;
    }

    public String getCheckDataPath() {
        return checkDataPath;
    }

    public void setCheckDataPath(String checkDataPath) {
        this.checkDataPath = checkDataPath;
    }

    public Sql getSql() {
        return sql;
    }

    public void setSql(Sql sql) {
        this.sql = sql;
    }

    public TargetDataLoad getTargetDataLoad() {
        return targetDataLoad;
    }

    public void setTargetDataLoad(TargetDataLoad targetDataLoad) {
        this.targetDataLoad = targetDataLoad;
    }

    public SourceDataMigrate getSourceDataMigrate() {
        return sourceDataMigrate;
    }

    public void setSourceDataMigrate(SourceDataMigrate sourceDataMigrate) {
        this.sourceDataMigrate = sourceDataMigrate;
    }

    public TargetDataCheckExisting getTargetDataCheckExisting() {
        return targetDataCheckExisting;
    }

    public void setTargetDataCheckExisting(TargetDataCheckExisting targetDataCheckExisting) {
        this.targetDataCheckExisting = targetDataCheckExisting;
    }


    /**
     * A 库源数据迁移配置
     */
    public static class SourceDataMigrate {
        private String startDate;  // 开始日期 yyyy-MM-dd
        private String endDate;    // 结束日期 yyyy-MM-dd
        private boolean enabled;   // 是否启用

        // Getters and Setters
        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * B 库数据校验配置
     */
    public static class TargetDataCheckExisting {
        private String startDate;  // 开始日期 yyyy-MM-dd
        private String endDate;    // 结束日期 yyyy-MM-dd
        private boolean enabled;   // 是否启用

        // Getters and Setters
        public String getStartDate() {
            return startDate;
        }

        public void setStartDate(String startDate) {
            this.startDate = startDate;
        }

        public String getEndDate() {
            return endDate;
        }

        public void setEndDate(String endDate) {
            this.endDate = endDate;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

}