package org.yangxin.kafka.kafkastudy.sample.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ConsumerThreadSample {
    public static final String TOPIC_NAME = "kafka_topic";

    /*
        这种类型是经典模式，每一个线程单独创建一个 KafkaConsumer，用于保证线程安全
     */

    public static void main(String[] args) throws InterruptedException {
        KafkaConsumerRunner r1 = new KafkaConsumerRunner();
        Thread t1 = new Thread(r1);
        t1.start();
        Thread.sleep(15000);
//        r1.shutdown();
    }

    @SuppressWarnings({"DuplicatedCode", "unused"})
    public static class KafkaConsumerRunner implements Runnable {
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final KafkaConsumer<String, String> consumer;

        public KafkaConsumerRunner() {
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", "localhost:9092");
            props.setProperty("group.id", "test");
            props.setProperty("enable.auto.commit", "false");
            props.setProperty("auto.commit.interval.ms", "1000");
            props.setProperty("session.timeout.ms", "30000");
            props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);

            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
            TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

            consumer.assign(Arrays.asList(p0, p1));
        }

        @Override
        public void run() {
            try {
                while (!closed.get()) {
                    // 处理消息
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
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
            } catch (Exception e) {
                if (!closed.get()) {
                    throw e;
                }
            } finally {
                consumer.close();
            }
        }

        public void shutdown() {
            closed.set(true);
            consumer.wakeup();
        }
    }
}
