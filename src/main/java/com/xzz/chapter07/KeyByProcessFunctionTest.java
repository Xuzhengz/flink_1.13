package com.xzz.chapter07;

import com.xzz.chapter06.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

        URL resourceAsStream = ClassLoader.getSystemClassLoader().getResource("UserBehavior.csv");

//        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        DataStreamSource<String> inputStream = env.readTextFile(resourceAsStream.getPath());

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Long(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp();
            }
        }));

        userBehaviorStream.keyBy(data -> data.getUserid())
                .process(new KeyedProcessFunction<Long, UserBehavior, String>() {
                    @Override
                    public void processElement(UserBehavior userBehavior, Context context, Collector<String> collector) throws Exception {
//                        collector.collect("当前数据的事件时间：" + userBehavior.getTimestamp());

                        //注册10S后的定时器
                        context.timerService().registerProcessingTimeTimer(context.timerService().currentProcessingTime() + 10 * 1000L);

                    }


                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect("定时器触发时间" + timestamp);
                    }
                }).print("timeserver：");


        env.execute();
    }
}