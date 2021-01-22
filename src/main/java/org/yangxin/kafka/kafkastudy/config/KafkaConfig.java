package org.yangxin.kafka.kafkastudy.config;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;

/**
 * @author yangxin
 * 1/22/21 6:16 PM
 */
@SuppressWarnings("DuplicatedCode")
@Configuration
public class KafkaConfig {

    private final KafkaProperties kafkaProperties;

    @Autowired
    public KafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public KafkaProducer<String, Object> kafkaProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        /*
            前置知识：
                分区中的所有副本统称为AR（Assigned Replicas）。
                所有与leader副本保持一定程度同步的副本（包括Leader）组成ISR（In-Sync Replicas），
                ISR集合是AR集合中的一个子集。

            acks: Broker完成生产者请求前需要确认的数量
            acks=0时，生产者不会等待确认，直接添加到套接字缓冲区等待发送；
            acks=1时，等待leader写到本地日志就行；
            acks=all或acks=-1时，等待isr中所有副本确认
            （注意：确认都是broker接收到消息，放入到内存后就直接返回确认，不是需要等待数据写入磁盘后才返回确认，这也是Kafka快的原因。
            这也就是Kafka在理论上不能保证消息百分百投递成功的原因。）
         */
        properties.put(ProducerConfig.ACKS_CONFIG, kafkaProperties.getAcksConfig());
        properties.put(ProducerConfig.RETRIES_CONFIG, "0");
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG, "16384");
        properties.put(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG, "33554432");

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");

        // Producer的主对象

        return new KafkaProducer<>(properties);
    }
}
