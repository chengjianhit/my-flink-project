package com.cheng.exercise;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCountFromWindow {

    public static void main(String[] args) throws Exception {

        //创建 execution environment
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //nc -l -p 9999
        DataStreamSource<String> textStream = environment.socketTextStream("localhost", 9999, "\n");
        DataStream<Tuple2<String, Integer>> windowCounts = textStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String word : value.split("\\s")) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        windowCounts.print().setParallelism(1);
        environment.execute("ChengJian WordCount");
    }
}
