package org.yangxin.kafka.kafkastudy.sample.admin;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

import java.util.*;
import java.util.concurrent.ExecutionException;

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

//        deleteTopic();

//        increasePartition(2);

        // 获取Topic列表
//        listTopic();

        describeTopics();

//        alterConfig();

//        describeConfig();
    }

    /**
     *  修改config属性
     */
    public static void alterConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
//        Map<ConfigResource, Config> configResourceConfigMap = new HashMap<>();
//
//        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        // 组织两个参数
//        Config config = new Config(Collections.singletonList(new ConfigEntry("preallocate", "true")));
//        configResourceConfigMap.put(configResource, config);
//
//        AlterConfigsResult alterConfigsResult = adminClient.alterConfigs(configResourceConfigMap);
//        alterConfigsResult.all().get();


        /*
            从2.3以上的版本新修改的API
         */
        Map<ConfigResource, Collection<AlterConfigOp>> configsMap = new HashMap<>();
        // 组织两个参数
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        AlterConfigOp alterConfigOp = new AlterConfigOp(new ConfigEntry("preallocate", "false"),
                AlterConfigOp.OpType.SET);
        configsMap.put(configResource, Collections.singletonList(alterConfigOp));

        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(configsMap);
        alterConfigsResult.all().get();
    }

    public static void describeConfig() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, TOPIC_NAME);
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(configResource));
        Map<ConfigResource, Config> configResourceConfigMap = describeConfigsResult.all().get();
        System.out.printf("configResourceConfigMap: [%s]%n", configResourceConfigMap);
    }

    /**
     * 创建Topic实例
     */
    public static void createTopic() throws InterruptedException, ExecutionException {
        AdminClient adminClient = adminClient();

        // 副本因子
        short replicationFactor = 1;
        NewTopic newTopic = new NewTopic(TOPIC_NAME, 1, replicationFactor);
        CreateTopicsResult topics = adminClient.createTopics(Collections.singletonList(newTopic));

        // adminClient.createTopics内部用了future机制，有可能子方法还未调用完毕，调用方法已经返回，导致topic创建失败
        // 暂时找不到更好的方法，只能让调用方法休眠2秒，以等待子方法执行完毕
//        TimeUnit.SECONDS.sleep(2);

        // 这是上面所提到的更好的方法（通过调用future的get方法）
        topics.all().get();
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

    /**
     * 删除Topic
     */
    public static void deleteTopic() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(TOPIC_NAME));
        deleteTopicsResult.all().get();
    }

    /**
     * 描述Topic
     */
    public static void describeTopics() throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(TOPIC_NAME));
        Map<String, TopicDescription> stringTopicDescriptionMap = describeTopicsResult.all().get();
//        System.out.println(stringTopicDescriptionMap);
        System.out.printf("stringTopicDescriptionMap: [%s]%n", stringTopicDescriptionMap);
    }

    /**
     * 增加partition数量
     */
    public static void increasePartition(int partitions) throws ExecutionException, InterruptedException {
        AdminClient adminClient = adminClient();

        Map<String, NewPartitions> partitionsMap = new HashMap<>();
        NewPartitions newPartitions = NewPartitions.increaseTo(partitions);
        partitionsMap.put(TOPIC_NAME, newPartitions);

        CreatePartitionsResult createPartitionsResult = adminClient.createPartitions(partitionsMap);
        createPartitionsResult.all().get();
    }
}
