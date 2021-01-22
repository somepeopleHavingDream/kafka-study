package org.yangxin.kafka.kafkastudy.common;

import lombok.Data;

import java.util.UUID;

/**
 * @author yangxin
 * 1/22/21 2:43 PM
 */
@Data
public class BaseResponseVO<M> {

    private String requestId;
    private M result;

    public static <M> BaseResponseVO<M> success() {
        BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
        baseResponseVO.setRequestId(generateRequestId());

        return baseResponseVO;
    }

    public static <M> BaseResponseVO<M> success(M result) {
        BaseResponseVO<M> baseResponseVO = new BaseResponseVO<>();
        baseResponseVO.setRequestId(generateRequestId());
        baseResponseVO.setResult(result);

        return baseResponseVO;
    }

    private static String generateRequestId() {
        return UUID.randomUUID().toString();
    }
}
