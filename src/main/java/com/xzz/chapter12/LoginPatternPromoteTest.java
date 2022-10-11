package com.xzz.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @create 2022-10-11 17:47
 * <p>
 * 改良后的CEP 检测登录失败
 */
public class LoginPatternPromoteTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        /**
         1、登录流
         CEP默认会对流里的时间戳进行排序。
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
        Pattern<LoginEvent, LoginEvent> loginFailPattern = Pattern.<LoginEvent>begin("fail")
                .where(new SimpleCondition<LoginEvent>() {
                    @Override
                    public boolean filter(LoginEvent loginEvent) throws Exception {
                        return "fail".equals(loginEvent.getEvent());
                    }
                }).times(3).consecutive(); //严格近邻 登录三次失败

        /**
         3、将模式应用在登录流中
         */
        PatternStream<LoginEvent> patternStream = CEP.pattern(loginStream.keyBy(LoginEvent::getUserid), loginFailPattern);


        /**
         4、将复杂组合事件结果输出
         */

        SingleOutputStreamOperator<String> warningStream = patternStream.process(new PatternProcessFunction<LoginEvent, String>() {
            @Override
            public void processMatch(Map<String, List<LoginEvent>> map, Context context, Collector<String> collector) throws Exception {
                // 提取三次登录失败时间
                LoginEvent fail1 = map.get("fail").get(0);
                LoginEvent fail2 = map.get("fail").get(1);
                LoginEvent fail3 = map.get("fail").get(2);

                collector.collect(fail1.getUserid() + " 连续三次失败！登录时间为：" + fail1.getTs() + "," + fail2.getTs() + "," + fail3.getTs());

            }
        });

        warningStream.print();


        env.execute();
    }
}
