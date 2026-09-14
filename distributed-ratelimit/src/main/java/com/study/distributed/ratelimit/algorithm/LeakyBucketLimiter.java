package com.study.distributed.ratelimit.algorithm;

import com.study.distributed.ratelimit.api.RateLimiter;

/**
 * 漏桶限流器
 *
 * 原理:
 * - 请求像水一样流入桶中
 * - 桶以固定速率 "漏水" (处理请求)
 * - 桶满时溢出的水被丢弃 (请求被拒绝)
 *
 * 学习要点:
 * - 与令牌桶的区别: 漏桶强制恒定速率，令牌桶允许突发
 * - 适合需要严格恒定速率的场景 (如数据库写入)
 * - 实现方式: 记录上次处理时间，按固定间隔处理
 */
public class LeakyBucketLimiter implements RateLimiter {

    private final long intervalMs;
    private volatile long lastProcessTime;
    private volatile long processedCount;

    /**
     * @param intervalMs 处理间隔 (毫秒), 即每 intervalMs 处理一个请求
     */
    public LeakyBucketLimiter(long intervalMs) {
        this.intervalMs = intervalMs;
        this.lastProcessTime = 0;
        this.processedCount = 0;
    }

    @Override
    public synchronized boolean tryAcquire() {
        long now = System.currentTimeMillis();

        if (now - lastProcessTime >= intervalMs) {
            lastProcessTime = now;
            processedCount++;
            return true;
        }

        return false;
    }

    @Override
    public String getStatus() {
        return "LeakyBucket: interval=" + intervalMs + "ms, processed=" + processedCount;
    }
}
