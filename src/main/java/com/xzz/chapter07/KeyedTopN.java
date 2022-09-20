package com.xzz.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/9/19-21:14
 *
 * 使用Keyed分组 统计TopN
 */
public class KeyedTopN {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> eventDataStreamSource = env.addSource(new CustomSource());

        SingleOutputStreamOperator<Event> stream = eventDataStreamSource.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
            @Override
            public long extractTimestamp(Event event, long l) {
                return event.getTimestamp();
            }
        }));

        //1、按照url分组，统计窗口内的每个url的访问量
        SingleOutputStreamOperator<Tuple2<String, Long>> WindowTopN = stream.keyBy(data -> data.getUrl())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCount(), new UrlTopWindow());

        WindowTopN.print();

        // 2、对于同一窗口统计出打分访问量进行排序

        env.execute();
    }
}
 class UrlCount implements AggregateFunction<Event, Long, Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Event event, Long aLong) {
        return aLong + 1;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return acc1 + aLong;
    }
}

 class UrlTopWindow extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

    @Override
    public void process(String s, Context context, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
        Long count = iterable.iterator().next();
        collector.collect(new Tuple2<>(s, count));
    }
}