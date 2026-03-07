package com.datastream.migration.engine;

import com.datastream.migration.handler.MigrationDataEvent;
import com.lmax.disruptor.EventFactory;

/**
 * 迁移数据事件工厂
 */
public class MigrationDataEventFactory implements EventFactory<MigrationDataEvent> {
    
    @Override
    public MigrationDataEvent newInstance() {
        return new MigrationDataEvent();
    }
}
