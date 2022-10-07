package com.xzz.chapter10;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @date 2022/8/26-16:03
 *
 * Flink 容错机制
 * 1）flink内部开启checkpoint + exactly once
 * 2）输入端：kafka
 * 3）输出端：开启2阶段提交
 */
public class FaultTolerance {
    public static void main(String[] args) throws Exception {
        /**
         * 启动时，还原至checkpoint的保存点
         */
        Configuration configuration = new Configuration();
        String save = "file:///C:/Users/xuzhengzhou/Desktop/checkpoint//cb824ca84d8ca3f0d37b63503b395096//chk-5";

        if (save != null && !"".equalsIgnoreCase(save.trim())) {
            configuration.setString("execution.savepoint.path", save);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(1);

        /**
         * 设置状态后端
         */
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        // 启用 checkpoint,设置触发间隔（两次执行开始时间间隔）
        env.enableCheckpointing(10000);
//        模式支持EXACTLY_ONCE()/AT_LEAST_ONCE()
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        存储位置，FileSystemCheckpointStorage(文件存储)
        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage("file:///C:/Users/xuzhengzhou/Desktop/checkpoint"));
//        超时时间，checkpoint没在时间内完成则丢弃
        env.getCheckpointConfig().setCheckpointTimeout(10000L);
//        同时并发数量
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
//        最小间隔时间（前一次结束时间，与下一次开始时间间隔）
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //RETAIN_ON_CANCELLATION:表示一旦Flink处理程序被cancel后，会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        //DELETE_ON_CANCELLATION: 表示一旦Flink处理程序被cancel后，会删除Checkpoint数据，只有job执行失败的时候才会保存checkpoint

        DataStreamSource<String> inputStream = env.socketTextStream("hadoop102", 7777);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndCount = inputStream.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
            String[] fields = line.split(" ");
            for (String field : fields) {
                out.collect(Tuple2.of(field, 1L));
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        SingleOutputStreamOperator<Tuple2<String, Long>> wordCount = wordAndCount.keyBy(line -> line.f0).sum(1);

        wordCount.print();


        env.execute();
    }
}