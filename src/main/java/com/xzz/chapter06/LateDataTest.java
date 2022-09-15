package com.xzz.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple3;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/9/14-20:12
 * <p>
 * <p>
 * Flink延迟数据三层保障
 * 1、watermark：调慢事件时间
 * 2、等待迟到数据时间
 * 3、迟到数据测输出流处理
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(2);


        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<UserBehavior> userBehaviorStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new UserBehavior(new Long(fields[0]), new Long(fields[1]), new Long(fields[2]), fields[3], new Long(fields[4]));
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp();
            }
        }));

        OutputTag<UserBehavior> late = new OutputTag<UserBehavior>("late") {
        };

        SingleOutputStreamOperator<Tuple2<Long,Long>> lateMainTainStream = userBehaviorStream.keyBy(data -> data.getUserid())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(late)
                .aggregate(new UvAgg(), new UvCountAndWindowTime());


        lateMainTainStream.print("Main：");
        lateMainTainStream.getSideOutput(late).print("迟到数据：");


        env.execute("late-test-job");
    }

    private static class UvAgg implements AggregateFunction<UserBehavior, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(UserBehavior userBehavior, Long aLong) {
            return aLong + 1;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    private static class UvCountAndWindowTime extends ProcessWindowFunction<Long, Tuple2<Long,Long>,Long, TimeWindow> {

        @Override
        public void process(Long aLong, Context context, Iterable<Long> iterable, Collector<Tuple2<Long, Long>> collector) throws Exception {
            Long count = iterable.iterator().next();
            collector.collect(new Tuple2<>(aLong,count));
        }
    }
}