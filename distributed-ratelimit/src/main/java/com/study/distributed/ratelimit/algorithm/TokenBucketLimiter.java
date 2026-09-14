package com.study.distributed.ratelimit.algorithm;

import com.study.distributed.ratelimit.api.RateLimiter;

/**
 * 令牌桶限流器
 *
 * 原理:
 * - 以固定速率向桶中添加令牌
 * - 每个请求消耗一个令牌
 * - 桶满时多余的令牌被丢弃
 * - 桶空时请求被拒绝
 *
 * 学习要点:
 * - 允许一定程度的突发流量 (桶中有积累的令牌)
 * - 长期来看限制平均速率
 * - 这是大多数 API 网关使用的限流算法
 * - Guava RateLimiter 就是令牌桶实现
 *
 * 令牌桶 vs 漏桶:
 *   令牌桶: 允许突发 (桶中积累多个令牌可以一次取走)
 *   漏桶:   严格恒定 (无论多少请求，都以固定速率处理)
 *
 * 场景举例:
 *   假设桶容量 10，每秒补充 1 个令牌
 *   空闲 10 秒后突然来 10 个请求:
 *   - 令牌桶: 10 个全部通过 (桶中有 10 个令牌)
 *   - 漏桶:   只有 1 个通过，其余排队等待
 */
public class TokenBucketLimiter implements RateLimiter {

    private final long maxTokens;
    private final long refillIntervalMs;

    private volatile long tokens;
    private volatile long lastRefillTime;

    /**
     * @param maxTokens 桶的最大容量
     * @param refillIntervalMs 令牌补充间隔 (毫秒)
     */
    public TokenBucketLimiter(long maxTokens, long refillIntervalMs) {
        this.maxTokens = maxTokens;
        this.refillIntervalMs = refillIntervalMs;
        this.tokens = maxTokens; // 初始满桶
        this.lastRefillTime = System.currentTimeMillis();
    }

    @Override
    public synchronized boolean tryAcquire() {
        refill();

        if (tokens > 0) {
            tokens--;
            return true;
        }

        return false;
    }

    /**
     * 补充令牌 - 根据时间流逝计算应补充的令牌数
     */
    private void refill() {
        long now = System.currentTimeMillis();
        long elapsed = now - lastRefillTime;

        if (elapsed >= refillIntervalMs) {
            long newTokens = elapsed / refillIntervalMs;
            tokens = Math.min(maxTokens, tokens + newTokens);
            lastRefillTime = now;
        }
    }

    @Override
    public String getStatus() {
        return "TokenBucket: " + tokens + "/" + maxTokens + " tokens (refill every " + refillIntervalMs + "ms)";
    }
}
