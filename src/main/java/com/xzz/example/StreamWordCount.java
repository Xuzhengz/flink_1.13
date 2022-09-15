package com.xzz.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author 徐正洲
 * @date 2022/8/26-16:03
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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