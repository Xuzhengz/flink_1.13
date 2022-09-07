package com.xzz.chapter05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/8/29-20:20
 */
public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> inputStream = env.fromElements(new Event("Mary", "./home", 3000l),
                new Event("Mary", "./home2", 1000l),
                new Event("Mary3", "./home3", 1000l),
                new Event("Mary", "./home5", 4000l)
        );

        /**
         * Map：映射：将数据流中的数据进行转换，形成新的数据流，就是“一”一“ 对应。
         */
//        1、匿名类对象
        SingleOutputStreamOperator<String> userStream = inputStream.map(new MapFunction<Event, String>() {
            @Override
            public String map(Event event) throws Exception {
                return event.getUser();
            }
        });
//        2、lambda表达式
        SingleOutputStreamOperator<String> userStream2 = inputStream.map(line -> {
            return line.getUser();
        });

        /**
         * filter：过滤：对每一个流内元素进行判断，true为输出，false则被过滤
         */
//        1、匿名类对象
        SingleOutputStreamOperator<Event> filterStream = inputStream.filter(new FilterFunction<Event>() {
            @Override
            public boolean filter(Event event) throws Exception {
                return "Mary".equals(event.getUser());
            }
        });
//        2、lambda表达式
        SingleOutputStreamOperator<Event> filterStream2 = inputStream.filter(data -> "Mary2".equals(data.getUser()));

        /**
         * flatMap：扁平映射：按照规则对数据进行打散拆分，在对拆分后的元素做转换处理
         */
//        1、匿名类对象
        SingleOutputStreamOperator<String> flatMapStream = inputStream.flatMap(new FlatMapFunction<Event, String>() {
            @Override
            public void flatMap(Event event, Collector<String> collector) throws Exception {
                collector.collect(event.getUser());
                collector.collect(event.getUrl());
                collector.collect(event.getTimestamp().toString());
            }
        });
//        2、lambda表达式
        SingleOutputStreamOperator<String> flatMapStream2 = inputStream.flatMap((Event event, Collector<String> out) -> {
            out.collect(event.getUser());
        }).returns(new TypeHint<String>() {
        });
//                .returns(TypeInformation.of(Types.STRING.getTypeClass()));

        /**
         * keyBy：按键分区后，聚合操作。
         */
        SingleOutputStreamOperator<Event> timestampStream = inputStream.keyBy(data -> data.getUser()).max("timestamp");


        /**
         * reduce：规约聚合
         */
//        1、统计每个用户的访问频次
        SingleOutputStreamOperator<Tuple2<String, Long>> reduceStream = inputStream.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1l);
            }
        }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                        return Tuple2.of(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1);
                    }
                });
//        2、选举当前最活跃的用户
        // 所有数据 都存入key分区，相当于全窗口
        SingleOutputStreamOperator<Tuple2<String, Long>> maxActiveUser = reduceStream.keyBy(data -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> stringLongTuple2, Tuple2<String, Long> t1) throws Exception {
                return stringLongTuple2.f1 > t1.f1 ? stringLongTuple2 : t1;
            }
        });

        /**
         * 物理分区
         */
//        1、shuffle 随机分区
//        inputStream.shuffle().print("shuffle").setParallelism(4);

//        2、rebalance 轮询分区
//        inputStream.rebalance().print("rebalance").setParallelism(10);

//        3、rescale 重缩放分区 分组后轮询分区
//        inputStream.rescale().print("rescale").setParallelism(4);

//        4、广播 将数据流的数据往下流都发送
//        inputStream.broadcast().print().setParallelism(4);

//        5、全局分区 将数据流合并输出至一个分区
//        inputStream.global().print().setParallelism(4);

//        6、自定义重分区
//        env.fromElements(1, 2, 3, 4, 5, 6, 7, 8)
//                .partitionCustom(new Partitioner<Integer>() {
//                    @Override
//                    public int partition(Integer o, int i) {
//                        return o % 2;
//                    }
//                }, new KeySelector<Integer, Integer>() {
//                    @Override
//                    public Integer getKey(Integer integer) throws Exception {
//                        return integer;
//                    }
//                }).print().setParallelism(4);


        /**
         * 水位线产生
         */
//        SingleOutputStreamOperator<Event> watermarkMonotonous = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//            @Override
//            public long extractTimestamp(Event event, long l) {
//                return event.getTimestamp();
//            }
//        }));
//
//
//        watermarkMonotonous.print();

//        SingleOutputStreamOperator<Event> boundWater = inputStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//            @Override
//            public long extractTimestamp(Event event, long l) {
//                return event.getTimestamp();
//            }
//        }));
//
//        boundWater.print();

//        自定义生成水位线




        env.execute();


    }
}