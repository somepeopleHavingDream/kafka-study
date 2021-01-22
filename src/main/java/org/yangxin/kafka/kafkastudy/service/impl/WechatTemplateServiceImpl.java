package org.yangxin.kafka.kafkastudy.service.impl;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
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

    /**
     * 文件类型
     */
    private static final int FILE_TYPE = 0;

    @Autowired
    public WechatTemplateServiceImpl(WechatTemplateProperties properties) {
        this.properties = properties;
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
