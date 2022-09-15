package com.xzz.chapter07;

import com.xzz.chapter06.UserBehavior;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/9/15-20:03
 * <p>
 * 处理函数：ProcessFunction
 */
public class ProcessFunctionExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        URL resourceAsStream = ClassLoader.getSystemClassLoader().getResource("UserBehavior.csv");

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102",7777);


        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Long(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp();
            }
        }));

        userBehaviorStream.process(new MyProcessFunction()).print("base：");


        env.execute();

    }

    private static class MyProcessFunction extends ProcessFunction<UserBehavior, String> {

        @Override
        public void processElement(UserBehavior userBehavior, Context context, Collector<String> collector) throws Exception {
            System.out.println(userBehavior.toString());
            System.out.println(context.timestamp());
            System.out.println(context.timerService().currentWatermark());
            System.out.println(getRuntimeContext().getIndexOfThisSubtask());
        }

    }
}