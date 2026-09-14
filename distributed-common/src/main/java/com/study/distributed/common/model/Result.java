package com.study.distributed.common.model;

import java.io.Serializable;

/**
 * 统一响应封装 - 使用 JDK 21 Record
 */
public record Result<T>(int code, String message, T data) implements Serializable {

    public static <T> Result<T> ok(T data) {
        return new Result<>(200, "success", data);
    }

    public static <T> Result<T> ok() {
        return new Result<>(200, "success", null);
    }

    public static <T> Result<T> fail(String message) {
        return new Result<>(500, message, null);
    }

    public static <T> Result<T> fail(int code, String message) {
        return new Result<>(code, message, null);
    }

    public boolean isSuccess() {
        return code == 200;
    }
}
