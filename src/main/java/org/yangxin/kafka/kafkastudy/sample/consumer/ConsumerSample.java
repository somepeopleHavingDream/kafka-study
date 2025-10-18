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
//        commitedOffsetWithPartition();
        // 手动订阅某个或某些分区，并提交 offset
//        commitedOffsetWithPartition2();
        // 手动指定 offset 的起始位置，及手动提交 offset
//        controlOffset();
        // 流量控制
        controlPause();
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

    /**
     * 手动提交 offset，并且手动控制 partition，更高级
     */
    private static void commitedOffsetWithPartition2() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 0,1 两个 partition
            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
            TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

            // 消费订阅某个 Topic 的某个分区
            consumer.assign(Collections.singletonList(p0));

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

    /**
     * 手动提交 offset 的起始位置，及手动提交 offset
     */
    private static void controlOffset() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 0,1 两个 partition
            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);

            // 消费订阅某个 Topic 的某个分区
            consumer.assign(Collections.singletonList(p0));

            while (true) {
                // 手动指定 offset 指定位置

                /*
                    1 人为控制 offset 起始位置
                    2 如果出现程序错误，重复消费一次
                 */

                /*
                    1 第一次从 0 消费【一般情况】
                    2 比如一次消费了 100 条，offset 置为 101 并且存入 redis
                    3 每次 poll 之前，从 redis 中获取最新的 offset 位置
                    4 每次从这个位置开始消费
                 */

                // 手动指定 offset 起始位置
                consumer.seek(p0, 400);

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

    /**
     * 流量控制 - 限流
     */
    private static void controlPause() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // 0,1 两个 partition
            TopicPartition p0 = new TopicPartition(TOPIC_NAME, 0);
            TopicPartition p1 = new TopicPartition(TOPIC_NAME, 1);

            // 消费订阅某个 Topic 的某个分区
            consumer.assign(Arrays.asList(p0, p1));

            long totalNum = 40;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                // 每个 partition 单独处理
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> pRecord = records.records(partition);
                    long num = 0;
                    for (ConsumerRecord<String, String> record : pRecord) {
                        System.out.printf("partition = %d offset = %d, key = %s, value = %s%n", record.partition(),
                                record.offset(), record.key(), record.value());

                        /*
                            1 接收到 record 信息后，去令牌桶中拿去令牌
                            2 如果获取到令牌，则继续业务处理
                            3 如果获取不到令牌，则 pause 等待令牌
                            4 当令牌桶中的令牌足够，则将 consumer 置为 resume 状态
                         */

                        num++;
                        if (record.partition() == 0) {
                            if (num >= totalNum) {
                                // pause 的作用是：下次 poll 的时候不再去拉 0 分区的数据，但是在这个例子里，poll 已经把所有的 record 都拉下来了
                                // 限流是一个持续的过程，如果一次就消费完了，而且不再产生了，那就不需要限流了
                                consumer.pause(Collections.singletonList(p0));
                            }
                        }
                        if (record.partition() == 1) {
                            if (num == 40) {
                                consumer.resume(Collections.singletonList(p0));
                            }
                        }
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
