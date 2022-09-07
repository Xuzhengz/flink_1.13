package com.xzz.chapter06;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;

/**
 * @author 徐正洲
 * @create 2022-09-07 17:44
 */
public class CustomPeriodicGenerator implements WatermarkGenerator<UserBehavior> {
    private Long delayTime = 5000L; // 延迟时间
    private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳

    @Override
    public void onEvent(UserBehavior event, long eventTimestamp, WatermarkOutput
            output) {
        // 每来一条数据就调用一次
        maxTs = Math.max(event.getTimestamp(), maxTs); // 更新最大时间戳
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        // 发射水位线，默认 200ms 调用一次
        output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
    }
}
