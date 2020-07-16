package com.boss.trainee.file.enums;

/**
 * @author: Jianbinbing
 * @Date: 2020/7/15 17:31
 */

public enum ErrorCodeEnum {


    ;
    /**
     * 响应状态码
     */
    private Integer code;
    /**
     * 响应信息
     */
    private String message;

    ErrorCodeEnum(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    public Integer getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }
}
