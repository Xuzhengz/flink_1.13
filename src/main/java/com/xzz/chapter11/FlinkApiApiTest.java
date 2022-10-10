package com.xzz.chapter11;

import com.xzz.chapter07.CustomSource;
import com.xzz.chapter07.Event;
import com.xzz.chapter09.BroadcastStateTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @author 徐正洲
 * @date 2022/10/7-20:30
 * <p>
 * Flink Table_API
 */
public class FlinkApiApiTest {
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
     * DataStream 转换成 表
     */
    @Test
    public void test() throws Exception {

        //创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //数据流转换成表
        Table table = tableEnv.fromDataStream(stream);

        //SQL查询数据
        Table resultTable = tableEnv.sqlQuery("select user,url,`timestamp` from " + table);


        //通过表API查询数据
        Table resultTable2 = table.select($("user"), $("url"), $("timestamp"))
                .where($("user").isEqual("wang"));

        //通过表环境转换成流打印数据
        tableEnv.toDataStream(resultTable).print("1");
        tableEnv.toDataStream(resultTable2).print("2");

        env.execute();


    }

    /**
     * Common Table API创建表执行环境
     */
    @Test
    public void test2() {
//        通过Blink计划器环境配置创建流处理表执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

//        使用老版本planner创建流处理表执行环境
        EnvironmentSettings settings2 = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useOldPlanner()
                .build();
        TableEnvironment tableEnv2 = TableEnvironment.create(settings2);

//        通过Blink计划器环境配置创建流处理表执行环境
        EnvironmentSettings settings3 = EnvironmentSettings.newInstance()
                .inBatchMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv3 = TableEnvironment.create(settings);

//        使用老版本planner创建流处理表执行环境
        EnvironmentSettings settings4 = EnvironmentSettings.newInstance()
                .inBatchMode()
                .useOldPlanner()
                .build();
        TableEnvironment tableEnv4 = TableEnvironment.create(settings2);
    }

    /**
     * Flink Table API创建表
     */
    @Test
    public void test3() {

        EnvironmentSettings settings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);


        String createDDl = "create table clickTable( " +
                "userid string, " +
                "itemid string, " +
                "categoryid string, " +
                "behavior string, " +
                "ts string) with (" +
                "'connector'='filesystem', " +
                "'format'='csv', " +
                "'path'='C:/Users/xuzhengzhou/IdeaProjects/test/flink_1.13/input/UserBehavior.csv' )";

//        注册表
        tableEnv.executeSql(createDDl);

//        调用Table API 进行表的查询转换
        Table clickTable = tableEnv.from("clickTable");
//        Table select = clickTable.where($("behavior").isEqual("pv"))
//                .select($("userid"), $("itemid"));
//
//        tableEnv.createTemporaryView("`result`",select);
//
//        Table result2 = tableEnv.sqlQuery("select userid , itemid from `result`");
//

        Table agg = tableEnv.sqlQuery("select userid,count(0) as cnt from clickTable group by userid");
        String outTable = "create table outTable (" +
                "userid string, " +
                "cnt bigint " +
                ") with (" +
                " 'connector' = 'print')";

//        注册表
        tableEnv.executeSql(outTable);



        //打印输出
        agg.executeInsert("outTable");





    }
}