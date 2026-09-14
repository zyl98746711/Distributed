package com.study.distributed.ratelimit.annotation;

import java.lang.annotation.*;

/**
 * 限流注解
 *
 * 使用示例:
 * @RateLimit(algorithm = Algorithm.TOKEN_BUCKET, maxRequests = 100, windowMs = 1000)
 * public String handleRequest() { ... }
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface RateLimit {

    enum Algorithm {
        FIXED_WINDOW,
        SLIDING_WINDOW,
        TOKEN_BUCKET,
        LEAKY_BUCKET
    }

    /**
     * 限流算法
     */
    Algorithm algorithm() default Algorithm.TOKEN_BUCKET;

    /**
     * 最大请求数 / 桶容量
     */
    long maxRequests() default 100;

    /**
     * 窗口大小 / 补充间隔 (毫秒)
     */
    long windowMs() default 1000;

    /**
     * 限流后的提示消息
     */
    String message() default "请求过于频繁，请稍后重试";
}
