package org.yangxin.kafka.kafkastudy.sample.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@SuppressWarnings("InfiniteLoopStatement")
public class ConsumerSample {
    private final static String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) {
        helloWorld();
    }

    private static void helloWorld() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 消费订阅哪一个 topic 或者几个 topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("partition = %d offset = %d, key = %s, value = %s%n", record.partition(),
                            record.offset(), record.key(), record.value());
                }
            }
        }
    }
}
