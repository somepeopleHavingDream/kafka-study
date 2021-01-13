package org.yangxin.kafka.kafkastudy.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 * @author yangxin
 * 1/13/21 2:20 PM
 */
@Slf4j
public class AdminSample {

    public static final String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) {
//        AdminClient adminClient = AdminSample.adminClient();
//        log.info("adminClient: [{}]", adminClient);

        createTopic();
    }

    /**
     * 设置AdminClient
     */
    public static AdminClient adminClient() {
        Properties properties = new Properties();
        // 9092是kafka的broker默认监听端口
        properties.setProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.3:9092");

        return AdminClient.create(properties);
    }

    /**
     * 创建Topic实例
     */
    public static void createTopic() {
        AdminClient adminClient = adminClient();

        // 副本因子
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic("", 1, replicationFactor);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));
        log.info("topics: [{}]", topics);
    }
}
