package com.xzz.chapter09;

import com.xzz.chapter07.CustomSource;
import com.xzz.chapter07.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * @author 徐正洲
 * @date 2022/9/21-20:54
 * <p>
 * 算子状态
 * 1、ListState列表状态
 * 2、UnionListState联合列表状态：将每个子任务的状态联合起来广播至下游
 * 3、BroadcastState广播状态
 */
public class OperatorStateTest {
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
     * 案例：自定义sink输出，缓存状态后输出
     */
    @Test
    public void test() throws Exception {

        stream.print();

        stream.addSink(new BuffSink(10));


        env.execute();
    }

    private class BuffSink implements SinkFunction<Event>, CheckpointedFunction {
        final int buff;
        List<Event> buffElement;
        ListState<Event> checkpointState;


        public BuffSink(int buff) {
            this.buff = buff;
            this.buffElement = new ArrayList<>();
        }


        @Override
        public void invoke(Event value, Context context) throws Exception {
            //数据缓存至列表
            buffElement.add(value);

            if (buffElement.size() == 10){
                for (Event event : buffElement) {
                    System.out.println(event + "    输出成功");
                }
                buffElement.clear();
            }


        }

        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            checkpointState.clear();
            //对状态持久化
            for (Event event : buffElement) {
                checkpointState.add(event);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext functionInitializationContext) throws Exception {
            //初始化算子状态
            ListStateDescriptor<Event> buffDescriptor = new ListStateDescriptor<>("buff", Event.class);

            checkpointState = functionInitializationContext.getOperatorStateStore().getListState(buffDescriptor);

            //故障恢复，需要将状态的所有元素复制到buff中,重组策略为：平均分配
            if (functionInitializationContext.isRestored()){
                for (Event event : checkpointState.get()) {
                    buffElement.add(event);
                }
            }

        }
    }
}