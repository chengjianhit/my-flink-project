package com.cheng.exercise;

import org.apache.commons.compress.archivers.dump.DumpArchiveEntry;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DataStreamJoinDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> textStream = env.socketTextStream("localhost", 9000, "\n");
        DataStream<Tuple2<String, String>> dataStream1 = textStream.map(input ->
                Tuple2.of(input, "from testStream1")
        ).returns(Types.TUPLE(Types.STRING, Types.STRING));

        DataStreamSource<String> textStream2 = env.socketTextStream("localhost", 9001, "\n");
        DataStream<Tuple2<String, String>> dataStream2 = textStream2.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return Tuple2.of(value, "from testStream2");
            }
        });

        DataStream<String> result = dataStream1.join(dataStream2)
                .where(t1 -> t1.getField(0)).equalTo(t2 -> t2.getField(0))
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply((t1, t2) -> t1.getField(0).toString() +"---"+ t1.getField(1) + "|" + t2.getField(1));
        result.print().setParallelism(1);

        env.execute("Cheng Join Test");


    }
}
