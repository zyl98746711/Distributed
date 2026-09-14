package com.study.distributed.ratelimit.algorithm;

import com.study.distributed.ratelimit.api.RateLimiter;

import java.util.ArrayDeque;
import java.util.Deque;

/**
 * 滑动窗口限流器 (日志实现)
 *
 * 原理: 记录每个请求的时间戳，滑动窗口内统计请求数
 *
 * 学习要点:
 * - 解决了固定窗口的边界问题
 * - 缺点: 需要存储每个请求的时间戳，内存占用大
 * - 改进: 可以用 Redis 的有序集合实现分布式版本
 */
public class SlidingWindowLimiter implements RateLimiter {

    private final int maxRequests;
    private final long windowMs;
    private final Deque<Long> timestamps = new ArrayDeque<>();

    /**
     * @param maxRequests 窗口内最大请求数
     * @param windowMs 窗口大小 (毫秒)
     */
    public SlidingWindowLimiter(int maxRequests, long windowMs) {
        this.maxRequests = maxRequests;
        this.windowMs = windowMs;
    }

    @Override
    public synchronized boolean tryAcquire() {
        long now = System.currentTimeMillis();
        long windowStart = now - windowMs;

        // 清除窗口外的时间戳
        while (!timestamps.isEmpty() && timestamps.peekFirst() <= windowStart) {
            timestamps.pollFirst();
        }

        if (timestamps.size() < maxRequests) {
            timestamps.addLast(now);
            return true;
        }

        return false;
    }

    @Override
    public String getStatus() {
        return "SlidingWindow: " + timestamps.size() + "/" + maxRequests + " (window=" + windowMs + "ms)";
    }
}
