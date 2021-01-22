package org.yangxin.kafka.kafkastudy.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.util.Optional;

/**
 * @author yangxin
 * 1/22/21 2:51 PM
 */
@Slf4j
public class FileUtil {

    public static String readFile(String filePath) {
//    public static String readFile(String filePath) throws IOException {
//        @Cleanup
        StringBuilder stringBuilder;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;

            // StringBuilder不是一个线程安全的类，但是在这里，作为局部变量来说，保证了线程安全
            stringBuilder = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                stringBuilder.append(line);
            }

            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public static Optional<JSONObject> readFile2JSONObject(String filePath) {
        String fileContent = readFile(filePath);
        log.info("fileContent: [{}]", fileContent);

        return Optional.ofNullable(JSON.parseObject(fileContent));
    }

    public static Optional<JSONArray> readFile2JSONArray(String filePath) {
        String fileContent = readFile(filePath);
        log.info("fileContent: [{}]", fileContent);

        return Optional.ofNullable(JSON.parseArray(fileContent));
    }
}
