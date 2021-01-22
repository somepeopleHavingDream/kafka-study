package org.yangxin.kafka.kafkastudy.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * @author yangxin
 * 1/22/21 4:48 PM
 */
@Data
@Configuration
@ConfigurationProperties(prefix = "template")
public class WechatTemplateProperties {

    private List<WechatTemplate> templateList;

    /**
     * 0：文件获取
     * 1：数据库获取
     * 2：es获取
     */
    private int templateResultType;

    private String templateResultFilePath;

    @Data
    public static class WechatTemplate {

        private String templateId;
        private String templateFilePath;
        private boolean active;
    }
}
