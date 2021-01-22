package org.yangxin.kafka.kafkastudy.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * @author yangxin
 * 1/22/21 6:14 PM
 */
@SuppressWarnings("SpellCheckingInspection")
@Data
@Configuration
@ConfigurationProperties(prefix = "wechat.kafka")
public class KafkaProperties {

    private String bootstrapServers;
    private String acksConfig;
    private String partitionerClass;
}
