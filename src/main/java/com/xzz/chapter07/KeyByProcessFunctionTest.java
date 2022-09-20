package com.xzz.chapter07;

import com.xzz.chapter06.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/9/15-20:46
 * <p>
 * 按键处理函数：可使用定时器调用。
 */
public class KeyByProcessFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomSource());

        SingleOutputStreamOperator<Event> eventWatermarkStream = eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));

        eventWatermarkStream.keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, Event, String>() {
                    @Override
                    public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
                        System.out.println("时间戳：" + context.timestamp());
                        System.out.println("水位线：" + context.timerService().currentWatermark());

                        context.timerService().registerEventTimeTimer(context.timestamp() + 3 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        System.out.println("定时器触发，当前Key：" + ctx.getCurrentKey());
                    }
                }).print("事件触发定时器：");


        env.execute();
    }
}