package org.yangxin.kafka.kafkastudy.controller;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import org.yangxin.kafka.kafkastudy.common.BaseResponseVO;
import org.yangxin.kafka.kafkastudy.config.WechatTemplateProperties;
import org.yangxin.kafka.kafkastudy.service.WechatTemplateService;
import org.yangxin.kafka.kafkastudy.util.FileUtil;

import java.util.Map;

/**
 * @author yangxin
 * 1/22/21 2:25 PM
 */
@SuppressWarnings("rawtypes")
@RestController
@RequestMapping("/v1")
public class WechatTemplateController {

//    private final WechatTemplateProperties properties;
    private final WechatTemplateService wechatTemplateService;

    @Autowired
    public WechatTemplateController(WechatTemplateService wechatTemplateService) {
//        this.properties = properties;
        this.wechatTemplateService = wechatTemplateService;
    }

    @GetMapping("/template")
    public BaseResponseVO getTemplate() {
        WechatTemplateProperties.WechatTemplate wechatTemplate = wechatTemplateService.getWechatTemplate();

        Map<String, Object> resultMap = Maps.newHashMap();
        resultMap.put("templateId", wechatTemplate.getTemplateId());
        resultMap.put("template", FileUtil.readFile2JSONArray(wechatTemplate.getTemplateFilePath()));

        return BaseResponseVO.success(resultMap);
    }

    @GetMapping("/template/result")
    public BaseResponseVO templateStatistics(@RequestParam(value = "templateId", required = false) String templateId) {
        JSONObject templateStatistics = wechatTemplateService.templateStatistics(templateId);
        return BaseResponseVO.success(templateStatistics);
    }

    @PostMapping("/template/report")
    public BaseResponseVO dataReported(@RequestBody String reportData) {
        wechatTemplateService.templateReported(JSON.parseObject(reportData));
        return BaseResponseVO.success();
    }
}
