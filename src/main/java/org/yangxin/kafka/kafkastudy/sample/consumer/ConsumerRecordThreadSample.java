package org.yangxin.kafka.kafkastudy.sample.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConsumerRecordThreadSample {
    private final static String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) throws InterruptedException {
        String brokerList = "localhost:9092";
        String groupId = "test";
        int workerNum = 5;

        ConsumerExecutor consumerExecutor = new ConsumerExecutor(brokerList, groupId, TOPIC_NAME);
        consumerExecutor.execute(workerNum);

        Thread.sleep(1000000);
        consumerExecutor.shutdown();
    }

    @SuppressWarnings("InfiniteLoopStatement")
    public static class ConsumerExecutor {
        private final KafkaConsumer<String, String> consumer;
        private ExecutorService executor;

        public ConsumerExecutor(String brokerList, String groupId, String topic) {
            Properties props = new Properties();
            props.setProperty("bootstrap.servers", brokerList);
            props.setProperty("group.id", groupId);
            props.setProperty("enable.auto.commit", "true");
            props.setProperty("auto.commit.interval.ms", "1000");
            props.setProperty("session.timeout.ms", "30000");
            props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
        }

        public void execute(int workerNum) {
            executor = new ThreadPoolExecutor(workerNum, workerNum, 0L, TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                for (ConsumerRecord<String, String> record : records) {
                    executor.submit(new ConsumerRecordWorker(record));
                }
            }
        }

        public void shutdown() {
            if (consumer != null) {
                consumer.close();
            }
            if (executor != null) {
                executor.shutdown();

                try {
                    if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                        System.out.println("Timeout... Ignore for this case");
                    }
                } catch (InterruptedException e) {
                    System.out.println("Other thread interrupted this shutdown, ignore for this case.");
                    Thread.currentThread().interrupt();
                }
            }
        }

        public static class ConsumerRecordWorker implements Runnable {
            private final ConsumerRecord<String, String> record;

            public ConsumerRecordWorker(ConsumerRecord<String, String> record) {
                this.record = record;
            }

            @Override
            public void run() {
                System.out.println("Thread " + Thread.currentThread().getName() + " is running...");
                System.out.printf("partition = %d, offset = %d, key = %s, value = %s%n", record.partition(),
                        record.offset(), record.key(), record.value());
            }
        }
    }
}
