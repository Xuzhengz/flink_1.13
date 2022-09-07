package com.xzz.chapter06;

import org.apache.flink.api.common.eventtime.*;

/**
 * @author 徐正洲
 * @create 2022-09-07 17:29
 */
public class CustomWatermark implements WatermarkStrategy<UserBehavior> {
    @Override
    public TimestampAssigner<UserBehavior> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp(); // 告诉程序数据源里的时间戳是哪一个字段
            }
        };
    }

    @Override
    public WatermarkGenerator<UserBehavior> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new CustomPeriodicGenerator();
    }
}



