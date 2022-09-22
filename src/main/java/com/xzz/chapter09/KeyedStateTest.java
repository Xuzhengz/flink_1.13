package com.xzz.chapter09;

import com.xzz.chapter07.CustomSource;
import com.xzz.chapter07.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/9/20-19:51
 * <p>
 * <p>
 * 介绍：按键分区状态的使用
 * 1、valueState：值状态
 * 2、listState：列表状态
 * 3、mapState：键值状态
 * 4、reduceState：规约状态，输入输出类型需要一直
 * 5、aggregatingState：聚合状态
 */
public class



KeyedStateTest {
    public static StreamExecutionEnvironment env = null;
    public static SingleOutputStreamOperator<Event> stream = null;

    static {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomSource());

        stream = eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));
    }

    /**
     * 各状态样例，按照键隔离状态
     */
    @Test
    public void test() throws Exception {
        stream.keyBy(data -> data.getUser())
                .flatMap(new MyFlatMap())
                .print();
        env.execute();
    }

    /**
     * 值状态案例：pv全数据统计，利用定时器
     */
    @Test
    public void test2() throws Exception {
        stream.keyBy(data -> data.getUser())
                .process(new MyKeyedProcess())
                .print("pv：");

        env.execute();

    }


    /**
     * 列表状态案例：模拟sql联表查询
     */
    @Test
    public void test3() throws Exception {
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream1 = env.fromElements(Tuple3.of("a", "stream-1", 1000L), Tuple3.of("b", "stream-1", 2000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));
        SingleOutputStreamOperator<Tuple3<String, String, Long>> stream2 = env.fromElements(Tuple3.of("a", "stream-2", 3000L), Tuple3.of("b", "stream-2", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, String, Long> stringStringLongTuple3, long l) {
                        return stringStringLongTuple3.f2;
                    }
                }));

        stream1.keyBy(data -> data.f0)
                .connect(stream2.keyBy(data -> data.f0))
                .process(new CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {

                    ListState<Tuple2<String, Long>> listState1 = null;
                    ListState<Tuple2<String, Long>> listState2 = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        listState1 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("list1", Types.TUPLE(Types.STRING, Types.LONG)));
                        listState2 = getRuntimeContext().getListState(new ListStateDescriptor<Tuple2<String, Long>>("list2", Types.TUPLE(Types.STRING, Types.LONG)));

                    }

                    @Override
                    public void processElement1(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
                        //获取另外一条流的状态，匹配输出

                        for (Tuple2<String, Long> list2 : listState2.get()) {
                            collector.collect(stringStringLongTuple3 + "=>" + list2);
                        }
                        listState1.add(Tuple2.of(stringStringLongTuple3.f0, stringStringLongTuple3.f2));
                    }

                    @Override
                    public void processElement2(Tuple3<String, String, Long> stringStringLongTuple3, Context context, Collector<String> collector) throws Exception {
                        for (Tuple2<String, Long> list1 : listState1.get()) {
                            collector.collect(stringStringLongTuple3 + "=>" + list1);
                        }
                        listState2.add(Tuple2.of(stringStringLongTuple3.f0, stringStringLongTuple3.f2));

                    }
                }).print("connect：");


        env.execute();
    }

    /**
     * 映射状态案例：滚动窗口
     */
    @Test
    public void test4() throws Exception {
        stream.print();

        stream.keyBy(data -> data.getUrl())
                .process(new FakeWindow(10000L))
                .print();


        env.execute();
    }

    /**
     * 规约状态案例：平均时间戳的统计
     */
    @Test
    public void test5() throws Exception {

        stream.keyBy(data -> data.getUser())
                .flatMap(new RichFlatMapFunction<Event, String>() {
                    AggregatingState<Event, Long> aggregatingState = null;
                    Long count;
                    ValueState<Long> valueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Tuple2<Long, Long>, Long>("agg", new AggregateFunction<Event, Tuple2<Long, Long>, Long>() {
                            @Override
                            public Tuple2<Long, Long> createAccumulator() {
                                return Tuple2.of(0L, 0L);
                            }

                            @Override
                            public Tuple2<Long, Long> add(Event event, Tuple2<Long, Long> longLongTuple2) {
                                return Tuple2.of(longLongTuple2.f0 + event.getTimestamp(), longLongTuple2.f1 + 1);
                            }

                            @Override
                            public Long getResult(Tuple2<Long, Long> longLongTuple2) {
                                return longLongTuple2.f0 / longLongTuple2.f1;
                            }

                            @Override
                            public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                                return null;
                            }
                        }, Types.TUPLE(Types.LONG, Types.LONG)));

                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value", Long.class));
                    }

                    @Override
                    public void flatMap(Event event, Collector<String> collector) throws Exception {
                        Long currentCount = valueState.value();

                        if (currentCount == null) {
                            currentCount = 1L;
                        } else {
                            currentCount++;
                        }


                        valueState.update(currentCount);
                        aggregatingState.add(event);

                        if (currentCount.equals(count)){
                            collector.collect(event.getUser() + "过去：");
                        }


                    }
                }).print();


        env.execute();

    }


    private class MyFlatMap extends RichFlatMapFunction<Event, String> {
        ValueState<Event> valueState = null;
        ListState<Event> listState = null;
        MapState<String, Long> mapState = null;
        ReducingState<Event> reducingState = null;
        AggregatingState<Event, String> aggregatingState = null;


        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> myValueState = new ValueStateDescriptor<>("myValueState", Event.class);
            valueState = getRuntimeContext().getState(myValueState);
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Event>("myListState", Event.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("myMapState", String.class, Long.class));
            reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<Event>("myReducingState", new ReduceFunction<Event>() {
                @Override
                public Event reduce(Event event, Event t1) throws Exception {
                    //更新时间戳
                    return new Event(event.getUser(), event.getUrl(), t1.getTimestamp());
                }
            }, Event.class));
            aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Event, Long, String>("myAggregatingState", new AggregateFunction<Event, Long, String>() {
                @Override
                public Long createAccumulator() {
                    return 0l;
                }

                @Override
                public Long add(Event event, Long aLong) {
                    return aLong + 1;
                }

                @Override
                public String getResult(Long aLong) {
                    return "count：" + aLong;
                }

                @Override
                public Long merge(Long aLong, Long acc1) {
                    return null;
                }
            }, Long.class));


            //ttl 目前只支持处理时间
            StateTtlConfig build = StateTtlConfig.newBuilder(Time.seconds(10L))
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();

            myValueState.enableTimeToLive(build);



        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            //访问和更新状态
//            System.out.println("前-值状态：" + valueState.value());
//            valueState.update(event);
//            System.out.println("后-值状态：" + valueState.value());
//
//            listState.add(event);

            mapState.put(event.getUser(), mapState.get(event.getUser()) == null ? 1 : mapState.get(event.getUser()) + 1);
            System.out.println("map：" + event.getUser() + "," + mapState.get(event.getUser()));

            reducingState.add(event);
            System.out.println("规约状态：" + reducingState.get());

            aggregatingState.add(event);
            System.out.println("聚合状态：" + aggregatingState.get());


        }
    }

    private class MyKeyedProcess extends KeyedProcessFunction<String, Event, String> {
        ValueState<Long> valueState = null;
        ValueState<Long> timeState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("valueState", Long.class));
            timeState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timeState", Long.class));

        }

        @Override
        public void processElement(Event event, Context context, Collector<String> collector) throws Exception {
            Long count = valueState.value();
            valueState.update(count == null ? 1 : count + 1);

            if (timeState.value() == null) {
                context.timerService().registerEventTimeTimer(event.getTimestamp() + 10 * 1000L);
                timeState.update(event.getTimestamp() + 10 * 1000L);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，输出统计结果
            out.collect(ctx.getCurrentKey() + "," + valueState.value());

            //清空状态
            timeState.clear();
        }
    }

    private class FakeWindow extends KeyedProcessFunction<String, Event, String> {
        Long windowSize = 0L;
        MapState<Long, Long> mapState;

        public FakeWindow(Long windowSize) {
            this.windowSize = windowSize;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("keyed-state", Long.class, Long.class));
        }

        @Override
        public void processElement(Event event, Context context, Collector<String> collector) throws Exception {

            //根据时间戳判断属于哪个窗口

            Long windowStart = event.getTimestamp() / windowSize * windowSize;
            Long windowEnd = windowStart + windowSize;


            //注册end-1的定时器
            context.timerService().registerEventTimeTimer(windowEnd - 1);

            //更新状态，进行增量聚合
            if (mapState.contains(windowStart)) {
                Long count = mapState.get(windowStart);
                mapState.put(windowStart, count + 1);
            } else {
                mapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp + 1;
            Long windowStart = windowEnd - windowSize;
            Long count = mapState.get(windowStart);

            out.collect("窗口：" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd)
                    + "  url：" + ctx.getCurrentKey() + "  count：" + count);

            mapState.remove(windowStart);

        }


    }

    private class MyRichFlatMapFunction<T, T1> {
        public MyRichFlatMapFunction(long count) {

        }
    }
}