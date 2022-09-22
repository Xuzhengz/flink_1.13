package com.xzz.chapter09;

import com.xzz.chapter07.CustomSource;
import com.xzz.chapter07.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-09-22 16:12
 * <p>
 * 广播状态
 */
public class BroadcastStateTest {
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

    @Test
    public void test() throws Exception {
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("xzz", "login"),
                new Action("xzz", "pay"),
                new Action("wentao", "login"),
                new Action("wentao", "order")
        );

        DataStreamSource<Pattern> patterntream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "order")
        );

        //定义广播状态描述器

        MapStateDescriptor<Void, Pattern> pattern = new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class));

        BroadcastStream<Pattern> broadcast = patterntream.broadcast(pattern);

        SingleOutputStreamOperator<Tuple2<String, Pattern>> matches = actionStream.keyBy(data -> data.getUserId())
                .connect(broadcast)
                .process(new PatternDetector());

        matches.print();

        env.execute();


    }

    /**
     * 用户行为PoJo
     */
    public static class Action {
        private String userId;
        private String action;


        public Action() {
        }

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public String getAction() {
            return action;
        }

        public void setAction(String action) {
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    /**
     * 行为模式
     */
    public static class Pattern {
        private String action1;
        private String action2;

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        public String getAction1() {
            return action1;
        }

        public void setAction1(String action1) {
            this.action1 = action1;
        }

        public String getAction2() {
            return action2;
        }

        public void setAction2(String action2) {
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }

    private class PatternDetector extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {
        // 定义一个键值状态，保存用户上一次行为

        ValueState<String> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("value", String.class));
        }

        @Override
        public void processElement(Action action, ReadOnlyContext readOnlyContext, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            ReadOnlyBroadcastState<Void, Pattern> patternBroadcastState = readOnlyContext.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            Pattern pattern = patternBroadcastState.get(null);
            String lastValue = valueState.value();

            if (pattern != null && lastValue != null) {
                if (pattern.getAction1().equals(lastValue) && pattern.getAction2().equals(action.getAction())){
                    collector.collect(new Tuple2<>(readOnlyContext.getCurrentKey(),pattern));
                }

                valueState.update(action.getAction());

            }
        }

        @Override
        public void processBroadcastElement(Pattern pattern, Context context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            BroadcastState<Void, Pattern> patternBroadcastState = context.getBroadcastState(new MapStateDescriptor<>("pattern", Types.VOID, Types.POJO(Pattern.class)));
            patternBroadcastState.put(null, pattern);
        }
    }
}
