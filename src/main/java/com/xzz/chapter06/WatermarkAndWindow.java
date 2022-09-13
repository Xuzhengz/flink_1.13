package com.xzz.chapter06;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.omg.CORBA.DATA_CONVERSION;

import java.net.URL;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @author 徐正洲
 * @create 2022-09-07 17:18
 * <p>
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
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
            @Override
            public long extractTimestamp(UserBehavior userBehavior, long l) {
                return userBehavior.getTimestamp() * 1000L;
            }
        }));

        /**
         * reduce 规约开窗聚合
         */
/*        SingleOutputStreamOperator<Tuple2<Long, Long>> tupleStream = userBehaviorStream.map(data -> {
            return Tuple2.of(data.getCategoryid(), 1L);
        }).returns(Types.TUPLE(Types.LONG, Types.LONG));

        SingleOutputStreamOperator<Tuple2<Long, Long>> resultStream = tupleStream.keyBy(line -> line.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<Tuple2<Long, Long>>() {
                    @Override
                    public Tuple2<Long, Long> reduce(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> t1) throws Exception {
                        return Tuple2.of(longLongTuple2.f0, longLongTuple2.f1 + t1.f1);
                    }
                }).returns(Types.TUPLE(Types.LONG, Types.LONG));

        resultStream.print();*/

        /**
         * aggreageFunction 增量聚合
         */
//        SingleOutputStreamOperator<Double> aggregateStream = userBehaviorStream.keyBy(data -> true)
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
//                .aggregate(new MyAggFunction());
//
//        userBehaviorStream.print();
//        aggregateStream.print();

        /**
         * 全窗口函数 ：所有数据集齐在输出
         */


//        userBehaviorStream.keyBy(data -> true)
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .process(new MyProcessWindowFunction())
//                .print();

        /**
         * Agg和全窗口两者相结合
         */

        userBehaviorStream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new UvAgg(), new UvCountResult())
                .print("both");

        env.execute();
    }

    private static class MyAggFunction implements AggregateFunction<UserBehavior, Tuple2<Long, HashSet<String>>, Double> {

        @Override
        public Tuple2<Long, HashSet<String>> createAccumulator() {
            return Tuple2.of(0L, new HashSet<>());
        }

        @Override
        public Tuple2<Long, HashSet<String>> add(UserBehavior userBehavior, Tuple2<Long, HashSet<String>> longHashSetTuple2) {

            longHashSetTuple2.f1.add(String.valueOf(userBehavior.getUserid()));

            return Tuple2.of(longHashSetTuple2.f0 + 1, longHashSetTuple2.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet<String>> longHashSetTuple2) {
            return (double) longHashSetTuple2.f0 / longHashSetTuple2.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
            return null;
        }
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<UserBehavior, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<UserBehavior> iterable, Collector<String> collector) throws Exception {
            HashSet<String> user = new HashSet<>();

            for (UserBehavior userBehavior : iterable) {
                user.add(String.valueOf(userBehavior.getUserid()));
            }

            int uv = user.size();

            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口：" + new Timestamp(start) + " ~ " + new Timestamp(end) + "  uv值为：" + uv);


        }
    }

    private static class UvAgg implements AggregateFunction<UserBehavior, HashSet<String>, Long> {

        @Override
        public HashSet<String> createAccumulator() {
            return new HashSet<>();
        }

        @Override
        public HashSet<String> add(UserBehavior userBehavior, HashSet<String> strings) {
            strings.add(String.valueOf(userBehavior.getUserid()));
            return strings;
        }

        @Override
        public Long getResult(HashSet<String> strings) {
            return new Long(strings.size());
        }

        @Override
        public HashSet<String> merge(HashSet<String> strings, HashSet<String> acc1) {
            return null;
        }
    }

    private static class UvCountResult extends ProcessWindowFunction<Long, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Long> iterable, Collector<String> collector) throws Exception {
            Long uv = iterable.iterator().next();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口：" + new Timestamp(start) + " ~ " + new Timestamp(end) + "  uv值为：" + uv);
        }
    }
}
