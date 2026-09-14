package com.study.distributed.ratelimit.api;

/**
 * 限流器接口
 *
 * 学习要点:
 * 限流是保护系统的重要手段:
 * - 防止突发流量压垮服务
 * - 保证服务质量 (SLA)
 * - 资源公平分配
 */
public interface RateLimiter {

    /**
     * 尝试获取一个许可
     * @return true=允许通过, false=被限流
     */
    boolean tryAcquire();

    /**
     * 获取当前限流状态描述
     */
    String getStatus();
}
