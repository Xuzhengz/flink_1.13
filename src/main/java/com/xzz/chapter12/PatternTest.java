package com.xzz.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @create 2022-10-10 9:21
 * <p>
 * CEP模式
 */
public class PatternTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /**
         1、登录流
         */
        SingleOutputStreamOperator<LoginEvent> loginStream = env.fromElements(new LoginEvent("user1", "192.168.0.1", "fail", 1000L),
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
                }));

        /**
         2、定义patter 模式
         */

//        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("first")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent loginEvent) throws Exception {
//                        return "fail".equals(loginEvent.getEvent());
//                    }
//                })
//                .next("second")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent loginEvent) throws Exception {
//                        return "fail".equals(loginEvent.getEvent());
//                    }
//                })
//                .next("third")
//                .where(new SimpleCondition<LoginEvent>() {
//                    @Override
//                    public boolean filter(LoginEvent loginEvent) throws Exception {
//                        return "fail".equals(loginEvent.getEvent());
//                    }
//                });


        /**
        使用量词
        */
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("first")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEvent());
                    }
                }).times(3);

        /**
         3、将模式应用在登录流中
         */
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream.keyBy(LoginEvent::getUserid), loginFailPattern);


        /**
         4、将复杂组合事件结果输出
         */

        SingleOutputStreamOperator<String> warnStream = patternStream.select(new PatternSelectFunction<LoginEvent, String>() {
            @Override
            public String select(Map<String, List<LoginEvent>> map) throws Exception {
                //三次登录失败事件提取
                LoginEvent firstFail = map.get("first").get(0);
                LoginEvent secondFail = map.get("first").get(1);
                LoginEvent thirdFail = map.get("first").get(2);

                return firstFail.getUserid() + " 连续三次失败！登录时间为：" + firstFail.getTs() + "," + secondFail.getTs() + "," + thirdFail.getTs();
            }
        });

        warnStream.print();


        env.execute();

    }
}
