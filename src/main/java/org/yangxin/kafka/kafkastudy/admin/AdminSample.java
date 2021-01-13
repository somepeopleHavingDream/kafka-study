package org.yangxin.kafka.kafkastudy.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * @author yangxin
 * 1/13/21 2:20 PM
 */
@Slf4j
public class AdminSample {

    public static final String TOPIC_NAME = "kafka_topic";

    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        AdminClient adminClient = AdminSample.adminClient();
//        log.info("adminClient: [{}]", adminClient);

//        createTopic();

        // 获取Topic列表
        listTopic();
    }

    /**
     * 创建Topic实例
     */
    public static void createTopic() throws InterruptedException {
        AdminClient adminClient = adminClient();

        // 副本因子
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, replicationFactor);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));

        // adminClient.createTopics内部用了future机制，有可能子方法还未调用完毕，调用方法已经返回，导致topic创建失败
        // 暂时找不到更好的方法，只能让调用方法休眠2秒，以等待子方法执行完毕
        TimeUnit.SECONDS.sleep(2);
    }

    /**
     * 获取Topic列表
     */
    public static void listTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
        listTopicsOptions.listInternal(true);
        ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
//        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> nameSet = listTopicsResult.names().get();
        Collection<TopicListing> topicListingCollection = listTopicsResult.listings().get();

        // 打印names
        nameSet.forEach(System.out::println);
        // 打印topicListingCollection
        topicListingCollection.forEach(System.out::println);

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
}
