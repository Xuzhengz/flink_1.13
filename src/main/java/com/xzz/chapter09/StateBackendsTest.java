package com.xzz.chapter09;

import com.xzz.chapter07.CustomSource;
import com.xzz.chapter07.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;

import java.time.Duration;

/**
 * @author 徐正洲
 * @date 2022/9/22-20:48
 * <p>
 * checkpoint+状态后端
 */
public class StateBackendsTest {
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

//        设置状态后端
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
//        开启 Checkpoint, 每隔 30 秒钟做一次 CK
        env.enableCheckpointing(30000L);
//        指定 CK 的一致性语义
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        设置任务关闭的时候保留最后一次 CK 数据
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));
//        设置访问 HDFS 的用户名
        System.setProperty("HADOOP_USER_NAME", "root");
//        设置状态后端存储路径
        checkpointConfig.setCheckpointStorage("file:///C:/Users/xuzhengzhou/Desktop/checkpoint");
    }

    public static void main(String[] args) throws Exception {
        stream.keyBy(data -> data.getUser())
                .window(TumblingEventTimeWindows.of(Time.seconds(3)))
                .process(new ProcessWindowFunction<Event, Tuple2<String, Long>, String, TimeWindow>() {
                    ValueState<Long> valueState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value", Long.class));
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Event> iterable, Collector<Tuple2<String, Long>> collector) throws Exception {
                        Long count = valueState.value();
                        if (count != null) {
                            collector.collect(Tuple2.of(s, count + 1L));
                        } else {
                            collector.collect(Tuple2.of(s, 1L));
                        }

                        valueState.update(count + 1L);

                    }
                }).print();

        env.execute();
    }
}