package com.xzz.chapter06;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URL;

/**
 * @author 徐正洲
 * @create 2022-09-07 17:18
 *
 * Flink中水位线和窗口
 */
public class WatermarkAndWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        URL resource = ClassLoader.getSystemClassLoader().getResource("UserBehavior.csv");
        DataStreamSource<String> inputStream = env.readTextFile(resource.getPath());

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Long(fields[2]), fields[3], new Long(fields[4]));
        });

        SingleOutputStreamOperator<UserBehavior> userBehaviorWatermarkStream = userBehaviorStream.assignTimestampsAndWatermarks(new CustomWatermark());

        userBehaviorWatermarkStream.print();

        env.execute();
    }
}
