package com.cheng.exercise.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaProducer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> text = env.addSource(new KafkaSource()).setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.0.108:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer("sunwu_test2", new SimpleStringSchema(), properties);
        producer.setWriteTimestampToKafka(true);
        text.addSink(producer);

        env.execute("Kafka Source");



    }
}
