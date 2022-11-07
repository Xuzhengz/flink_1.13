package com.xzz.example;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-11-07 9:22
 */
public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<Student> ds1 = env.socketTextStream("hadoop102", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String s, long l) {
                                String[] fields = s.split(",");
                                return new Long(fields[2]) * 1000L;
                            }
                        }))
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Student(fields[0], fields[1], new Long(fields[2]));
                });
        SingleOutputStreamOperator<Student> ds2 = env.socketTextStream("hadoop102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                            @Override
                            public long extractTimestamp(String s, long l) {
                                String[] fields = s.split(",");
                                return new Long(fields[2]) * 1000L;
                            }
                        }))
                .map(line -> {
                    String[] fields = line.split(",");
                    return new Student(fields[0], fields[1], new Long(fields[2]));
                });

        SingleOutputStreamOperator<Tuple2> result = ds1.keyBy(data -> data.getId())
                .intervalJoin(ds2.keyBy(data -> data.getId()))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<Student, Student, Tuple2>() {
                    @Override
                    public void processElement(Student student, Student student2, Context context, Collector<Tuple2> collector) throws Exception {
                        collector.collect(new Tuple2(student, student2));
                    }
                });

        result.print("interjoin >>>>>>");

        env.execute();
    }

}
