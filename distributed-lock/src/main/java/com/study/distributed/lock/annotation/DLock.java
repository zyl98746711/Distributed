package com.study.distributed.lock.annotation;

import java.lang.annotation.*;
import java.time.Duration;

/**
 * 分布式锁注解
 *
 * 使用示例:
 * @DLock(key = "'order:' + #orderId", ttl = 30)
 * public void processOrder(String orderId) { ... }
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface DLock {

    /**
     * 锁的 key (支持 SpEL 表达式)
     */
    String key();

    /**
     * 锁的 TTL (秒)
     */
    long ttl() default 30;

    /**
     * 等待获取锁的超时时间 (秒), 0 表示不等待
     */
    long waitTime() default 0;

    /**
     * 获取失败时的错误消息
     */
    String message() default "操作过于频繁，请稍后重试";
}
