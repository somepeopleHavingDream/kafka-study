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
        // acks=all，这意味着leader需要等待所有备份都成功写入日志，这种策略会保证只要有一个备份存活就不会丢失数据，这是最强的保证。
        // acks=1，这意味着至少要等待leader已经成功将数据写入本地日志（这个地方的“本地日志”估计不是磁盘，不然说不通啊，已经写入到本地日志，怎么会挂掉丢失数据了呢？）
        // ，但是并没有等待所有follower是否成功写入，这种情况下，如果follower没有成功备份数据，而此时leader又挂掉，则消息会丢失。
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
