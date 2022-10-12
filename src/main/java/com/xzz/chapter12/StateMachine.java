package com.xzz.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import java.time.Duration;

/**
 * @author 徐正洲
 * @create 2022-10-12 10:48
 * <p>
 * CEP 状态机
 */
public class StateMachine {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /**
         1、登录流
         */
        KeyedStream<LoginEvent, String> loginStream = env.fromElements(new LoginEvent("user1", "192.168.0.1", "fail", 1000L),
                new LoginEvent("user1", "192.168.0.1", "fail", 2000L),
                new LoginEvent("user2", "192.168.0.1", "fail", 3000L),
                new LoginEvent("user1", "192.168.0.2", "fail", 4000L),
                new LoginEvent("user2", "192.168.0.2", "success", 5000L),
                new LoginEvent("user2", "192.168.0.2", "fail", 6000L),
                new LoginEvent("user2", "192.168.0.2", "fail", 7000L)

        ).
                assignTimestampsAndWatermarks(WatermarkStrategy.<LoginEvent>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<LoginEvent>() {
                    @Override
                    public long extractTimestamp(LoginEvent loginEvent, long l) {
                        return loginEvent.getTs();
                    }
                })).keyBy(LoginEvent::getUserid);

        /**
         2、数据按照顺序依次输入，用状态机进行处理，状态跳转
         */

        SingleOutputStreamOperator<String> warnStream = loginStream.flatMap(new StateMachineMapper());

        warnStream.print();
        env.execute();

    }

    public static class StateMachineMapper extends RichFlatMapFunction<LoginEvent, String> {
        //声明状态机当前状态

        ValueState<State> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<State>("value", State.class));
        }

        @Override
        public void flatMap(LoginEvent loginEvent, Collector<String> collector) throws Exception {
            //如果状态为空，进行初始化
            State value = valueState.value();

            if (value==null){
                value = State.Inital;
            }

            //跳转到下一个状态

            State nextState = value.transition(loginEvent.getEvent());

            //判断当前状态的特效情况，直接进行跳转

            if (nextState == State.Matched){
                // 检测到了匹配，输出报警信息
                collector.collect(loginEvent.getUserid() + "，连续三次登录失败");
            }else if (nextState == State.Terminal){
                valueState.update(State.Inital);
            }else {
                valueState.update(nextState);
            }

        }
    }

    /**
    状态机实现
    */

    public enum State{
        Terminal,   //终止状态
        Matched,   // 匹配成功

        /**
        S2状态处理，包含 当前事件类型，转移目标的状态
        */

        S2(new Transfer("fail",Matched),new Transfer("success",Terminal)),

        S1(new Transfer("fail",S2),new Transfer("success",Terminal)),

        //初始状态
        Inital(new Transfer("fail",S1),new Transfer("success",Terminal)),
        ;

        private Transfer[] transfers;
        State(Transfer... transfers){
           this.transfers =transfers;
        }

        /**
         状态的转移方法
        */
        public State transition(String eventType){
            for (Transfer transfer : transfers) {
                if (transfer.eventType.equals(eventType))
                    return transfer.getState();
            }
            //回到初始状态
            return Inital;
        }


    }
    public static class Transfer{
        public String eventType;
        public State state;

        public Transfer(String eventType, State state) {
            this.eventType = eventType;
            this.state = state;
        }

        public String getEventType() {
            return eventType;
        }

        public void setEventType(String eventType) {
            this.eventType = eventType;
        }

        public State getState() {
            return state;
        }

        public void setState(State state) {
            this.state = state;
        }
    }
}
