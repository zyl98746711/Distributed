package com.study.distributed.common.exception;

/**
 * 限流异常
 */
public class RateLimitException extends RuntimeException {

    public RateLimitException() {
        super("请求过于频繁，已被限流");
    }

    public RateLimitException(String message) {
        super(message);
    }
}
