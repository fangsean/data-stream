package com.datastream.migration.handler;

import com.datastream.migration.model.MigrationData;

import java.util.List;

/**
 * 迁移数据事件
 */
public class MigrationDataEvent {
    
    private List<MigrationData> dataList;
    
    public List<MigrationData> getDataList() {
        return dataList;
    }
    
    public void setDataList(List<MigrationData> dataList) {
        this.dataList = dataList;
    }
}
