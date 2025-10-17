package org.yangxin.kafka.kafkastudy.sample.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

@SuppressWarnings({"InfiniteLoopStatement", "DuplicatedCode", "unused"})
public class ConsumerSample {
    private final static String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) {
        // 自动提交
//        helloWorld();
        // 手动提交 offset
//        commitedOffset();
        // 手动对每个 partition 进行提交
        commitedOffsetWithPartition();
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

    /**
     * 手动提交 offset
     */
    private static void commitedOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
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

                // 手动通知 offset 提交
                consumer.commitAsync();
            }
        }
    }

    /**
     * 手动提交 offset，并且手动控制 partition
     */
    private static void commitedOffsetWithPartition() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 消费订阅哪一个 topic 或者几个 topic
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                // 每个 partition 单独处理
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                    for (ConsumerRecord<String, String> record : pRecord) {
                        System.out.printf("partition = %d offset = %d, key = %s, value = %s%n", record.partition(),
                                record.offset(), record.key(), record.value());
                    }

                    long lastOffset = pRecord.get(pRecord.size() - 1).offset();
                    // 单个 partition 中的 offset ，并且进行提交
                    Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
                    offset.put(partition, new OffsetAndMetadata(lastOffset + 1));
                    // 提交 offset
                    consumer.commitSync(offset);

                    System.out.println("partition = " + partition);
                }
            }
        }
    }
}
