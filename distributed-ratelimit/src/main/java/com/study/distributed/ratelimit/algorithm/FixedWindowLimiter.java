package com.study.distributed.ratelimit.algorithm;

import com.study.distributed.ratelimit.api.RateLimiter;

/**
 * 固定窗口限流器
 *
 * 原理: 将时间划分为固定窗口，每个窗口内允许固定数量的请求
 *
 * 学习要点:
 * - 优点: 实现简单，内存占用小
 * - 缺点: 窗口边界处可能出现 2 倍流量
 *   例如: 窗口大小 1s，限制 100 次
 *   在 0.9s 时刻来了 100 次，1.1s 时刻又来了 100 次
 *   0.2s 内通过了 200 次请求!
 *
 * 时间线图:
 *   |←─── 窗口1 ───→|←─── 窗口2 ───→|
 *   0s             1s              2s
 *              ↑0.9s↑1.1s↑
 *              100次 100次  ← 0.2s内通过200次!
 */
public class FixedWindowLimiter implements RateLimiter {

    private final int maxRequests;
    private final long windowMs;

    private volatile long windowStart;
    private volatile int counter;

    /**
     * @param maxRequests 窗口内最大请求数
     * @param windowMs 窗口大小 (毫秒)
     */
    public FixedWindowLimiter(int maxRequests, long windowMs) {
        this.maxRequests = maxRequests;
        this.windowMs = windowMs;
        this.windowStart = System.currentTimeMillis();
        this.counter = 0;
    }

    @Override
    public synchronized boolean tryAcquire() {
        long now = System.currentTimeMillis();

        // 检查是否进入新窗口
        if (now - windowStart >= windowMs) {
            windowStart = now;
            counter = 0;
        }

        if (counter < maxRequests) {
            counter++;
            return true;
        }

        return false;
    }

    @Override
    public String getStatus() {
        return "FixedWindow: " + counter + "/" + maxRequests + " (window=" + windowMs + "ms)";
    }
}
