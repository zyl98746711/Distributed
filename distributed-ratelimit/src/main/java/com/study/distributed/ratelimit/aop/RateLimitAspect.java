package com.study.distributed.ratelimit.aop;

import com.study.distributed.common.exception.RateLimitException;
import com.study.distributed.ratelimit.algorithm.*;
import com.study.distributed.ratelimit.annotation.RateLimit;
import com.study.distributed.ratelimit.api.RateLimiter;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 限流 AOP 切面
 *
 * 学习要点:
 * 通过注解 + AOP 实现声明式限流
 * 每个方法对应一个独立的限流器实例
 */
@Aspect
public class RateLimitAspect {

    private static final Logger log = LoggerFactory.getLogger(RateLimitAspect.class);

    /** 方法签名 -> 限流器实例 */
    private final Map<String, RateLimiter> limiters = new ConcurrentHashMap<>();

    @Around("@annotation(rateLimit)")
    public Object around(ProceedingJoinPoint joinPoint, RateLimit rateLimit) throws Throwable {
        String key = joinPoint.getSignature().toShortString();
        RateLimiter limiter = limiters.computeIfAbsent(key, k -> createLimiter(rateLimit));

        if (!limiter.tryAcquire()) {
            log.warn("请求被限流: {} - {}", key, limiter.getStatus());
            throw new RateLimitException(rateLimit.message());
        }

        return joinPoint.proceed();
    }

    private RateLimiter createLimiter(RateLimit annotation) {
        return switch (annotation.algorithm()) {
            case FIXED_WINDOW -> new FixedWindowLimiter((int) annotation.maxRequests(), annotation.windowMs());
            case SLIDING_WINDOW -> new SlidingWindowLimiter((int) annotation.maxRequests(), annotation.windowMs());
            case TOKEN_BUCKET -> new TokenBucketLimiter(annotation.maxRequests(), annotation.windowMs());
            case LEAKY_BUCKET -> new LeakyBucketLimiter(annotation.windowMs() / annotation.maxRequests());
        };
    }
}
