package com.xzz.chapter07;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;

/**
 * @author 徐正洲
 * @date 2022/9/19-20:31
 * <p>
 * Top N 案例：统计10S内最热门的两个url链接，并且5秒更新一次。
 */
public class TopNTest {
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
//        SingleOutputStreamOperator<Tuple2<String, Long>> WindowTopN = stream.keyBy(data -> data.getUrl())
//                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
//                .aggregate(new UrlCount(), new UrlTopWindow());
//
//        WindowTopN.print();

        // 2、对于同一窗口统计出打分访问量进行排序


        // 直接开窗，收集所有数据排序
        stream.map(data -> data.getUrl())
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCount(), new UrlTopNResult()).print();


        env.execute("topN_Job");
    }

    private static class UrlCount implements AggregateFunction<Event, Long, Long> {
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

    private static class UrlTopWindow extends ProcessWindowFunction<Long, Tuple2<String, Long>, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Long> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
            Long count = iterable.iterator().next();
            collector.collect(new Tuple2<>(s, count));
        }
    }

    private static class UrlHashMapCount implements AggregateFunction<String, HashMap<String, Long>, ArrayList<Tuple2<String, Long>>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(String s, HashMap<String, Long> stringLongHashMap) {
            Long count = stringLongHashMap.get(s);
            if (stringLongHashMap.containsKey(s)) {
                stringLongHashMap.put(s, count + 1);
            } else {
                stringLongHashMap.put(s, 1L);
            }
            return stringLongHashMap;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> stringLongHashMap) {
            ArrayList<Tuple2<String, Long>> tuple2s = new ArrayList<>();
            for (String key : stringLongHashMap.keySet()) {
                tuple2s.add(Tuple2.of(key, stringLongHashMap.get(key)));
            }
            tuple2s.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            return tuple2s;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }

    private static class UrlTopNResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Long>>, String, TimeWindow> {

        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<String> collector) throws Exception {
            StringBuilder result = new StringBuilder();
            result.append("---------------------\n");
            ArrayList<Tuple2<String, Long>> list = iterable.iterator().next();
            result.append("窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n");


            //取List前两个

            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> currentTuple = list.get(i);
                String info = "No." + (i + 1) + " "
                        + "url：" + currentTuple.f0 + " "
                        + "访问量：" + currentTuple.f1 + "\n";
                result.append(info);
            }
            result.append("---------------------");

            collector.collect(result.toString());
        }
    }
}