package com.xzz.chapter12;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author 徐正洲
 * @create 2022-10-12 10:21
 * <p>
 * 订单超时处理
 */
public class OrderTimeout {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /**
         1、数据流
         */
        SingleOutputStreamOperator<Order> orderStream = env.fromElements(new Order("user_1", "order_1", "create", 1000L),
                new Order("user_2", "order_2", "create", 2000L),
                new Order("user_1", "order_1", "modify", 10 * 1000L),
                new Order("user_1", "order_1", "pay", 60 * 1000L),
                new Order("user_2", "order_3", "create", 10 * 60 * 1000L),
                new Order("user_2", "order_3", "pay", 20 * 60 * 1000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Order>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Order>() {
            @Override
            public long extractTimestamp(Order order, long l) {
                return order.getTs();
            }
        }));

        /**
         2、定义模式
         */

        Pattern<Order, Order> timeoutPattern = Pattern.<Order>begin("create")
                .where(new SimpleCondition<Order>() {
                    @Override
                    public boolean filter(Order order) throws Exception {
                        return "create".equals(order.getEventType());
                    }
                }).followedBy("pay")
                .where(new SimpleCondition<Order>() {
                    @Override
                    public boolean filter(Order order) throws Exception {
                        return "pay".equals(order.getEventType());
                    }
                }).within(Time.minutes(15));

        /**
         3、将模式应用到数据流上
         */

        PatternStream<Order> patternStream = CEP.pattern(orderStream.keyBy(Order::getOrderId), timeoutPattern);
        OutputTag<String> outputTag = new OutputTag<String>("timeout") {
        };

        SingleOutputStreamOperator<String> result = patternStream.process(new TimeOutProcessFunction());

        result.print("支付成功");
        result.getSideOutput(outputTag).print("超时");


        env.execute();
    }

    public static class TimeOutProcessFunction extends PatternProcessFunction<Order, String> implements TimedOutPartialMatchHandler<Order> {

        @Override
        public void processMatch(Map<String, List<Order>> map, Context context, Collector<String> collector) throws Exception {
            Order pay = map.get("pay").get(0);
            collector.collect("用户：" + pay.getUserId() + "，订单Id：" + pay.getOrderId() + "，支付成功");
        }

        @Override
        public void processTimedOutMatch(Map<String, List<Order>> map, Context context) throws Exception {
            Order create = map.get("create").get(0);
            OutputTag<String> outputTag = new OutputTag<String>("timeout") {
            };
            context.output(outputTag, "用户：" + create.getUserId() + "，订单Id：" + create.getOrderId() + "，超时！支付失败!!!");
        }
    }
}
