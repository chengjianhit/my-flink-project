package com.cheng.exercise;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class DataStreamApiTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9999, "\n");

//        DataStream<String> result = textStream.flatMap((String input, Collector<String> collector) -> {
//            for (String ele : input.split(" ")) {
//                collector.collect(ele);
//            }
//        })
//                .returns(Types.STRING);
//
//        result.print().setParallelism(1);

        DataStream<Tuple2<String, Integer>> sumResult = textStream
                .map(line -> Tuple2.of(line.trim(), 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .timeWindow(Time.of(10, TimeUnit.SECONDS))
                .reduce((Tuple2<String, Integer> t1, Tuple2<String, Integer> t2)->
                    new Tuple2(t1.f0, t1.f1 +t2.f1)
                );
        sumResult.print().setParallelism(1);

        env.execute();


    }
}
