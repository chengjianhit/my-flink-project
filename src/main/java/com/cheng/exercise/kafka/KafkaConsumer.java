package com.cheng.exercise.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaConsumer {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.108:9092");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<String>("sunwu_test2", new SimpleStringSchema(), properties);
//        consumer.setStartFromEarliest();
//        consumer.setStartFromTimestamp(1563071417L);
        consumer.setStartFromLatest();
        DataStreamSource<String> stream = env.addSource(consumer);

        stream.print();

        env.setParallelism(1).execute("Kafka Consumer");


    }
}
