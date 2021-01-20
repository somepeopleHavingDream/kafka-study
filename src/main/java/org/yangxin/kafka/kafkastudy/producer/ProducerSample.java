package org.yangxin.kafka.kafkastudy.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author yangxin
 * 1/20/21 1:44 PM
 */
public class ProducerSample {

    public static final String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) {
        // producer异步发送
        producerSend();
    }

    /**
     * Producer异步发送演示
     */
    public static void producerSend() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.3:9092");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // 消息对象：ProducerRecorder
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC_NAME,
                    "key-" + i, "value-" + i);
            producer.send(record);
        }

        // 所有的打开的通道需要关闭
        producer.close();
    }
}
