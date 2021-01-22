package org.yangxin.kafka.kafkastudy.service.impl;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.yangxin.kafka.kafkastudy.config.WechatTemplateProperties;
import org.yangxin.kafka.kafkastudy.service.WechatTemplateService;
import org.yangxin.kafka.kafkastudy.util.FileUtil;

import java.util.List;
import java.util.Optional;

/**
 * @author yangxin
 * 1/22/21 2:27 PM
 */
@SuppressWarnings("StatementWithEmptyBody")
@Slf4j
@Service
public class WechatTemplateServiceImpl implements WechatTemplateService {

    private final WechatTemplateProperties properties;

    private final Producer<String, Object> producer;

    /**
     * 文件类型
     */
    private static final int FILE_TYPE = 0;

    @Autowired
    public WechatTemplateServiceImpl(WechatTemplateProperties properties, Producer<String, Object> producer) {
        this.properties = properties;
        this.producer = producer;
    }

    @Override
    public WechatTemplateProperties.WechatTemplate getWechatTemplate() {
        List<WechatTemplateProperties.WechatTemplate> templateList = properties.getTemplateList();
        Optional<WechatTemplateProperties.WechatTemplate> wechatTemplateOptional = templateList.stream()
                .filter(WechatTemplateProperties.WechatTemplate::isActive)
                .findFirst();

        return wechatTemplateOptional.orElse(null);
    }

    @Override
    public void templateReported(JSONObject reportInfo) {
        // Kafka Producer将数据推送至Kafka Topic
        log.info("reportedInfo: [{}]", reportInfo);

        // topic名
        String topicName = "topic-template";

        // 发送Kafka数据
        String templateId = reportInfo.getString("templateId");
        JSONArray reportData = reportInfo.getJSONArray("result");

        // 如果templateId相同，后续在统计分析时，可以考虑将相同id的内容放入同一个partition，便于分析使用
        ProducerRecord<String, Object> record = new ProducerRecord<>(topicName, templateId, reportData);

        /*
            1. Kafka Producer是线程安全的，建议多线程复用。如果每个线程都创建，会出现大量的上下文切换或争抢的情况，影响Kafka效率。
            2. ProducerRecord的key是一个很重要的内容：
                2.1. 我们可以根据key完成partition的负载均衡
                2.2. 合理的key设计，可以让Flink、Spark、Streaming之类的实时分析工具做更快速的处理
            3. ack = all，Kafka层面上就已经有了具有一次的消息投递保障，但是如果想真的不丢失诗句，最好自行处理异常
         */
        try {
            producer.send(record);
        } catch (Exception e) {
            // 将数据加入重发队列、redis、es之类……
        }
    }

    @Override
    public JSONObject templateStatistics(String templateId) {
        // 判断数据结果获取类型
        // 文件获取
        if (properties.getTemplateResultType() == FILE_TYPE) {
            return FileUtil.readFile2JSONObject(properties.getTemplateResultFilePath()).orElse(null);
        } else {
            // DB...
        }

        return null;
    }
}
