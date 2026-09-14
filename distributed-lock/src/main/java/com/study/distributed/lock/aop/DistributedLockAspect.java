package com.study.distributed.lock.aop;

import com.study.distributed.common.exception.DistributedLockException;
import com.study.distributed.lock.annotation.DLock;
import com.study.distributed.lock.api.DistributedLock;
import com.study.distributed.lock.api.DistributedLockFactory;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * 分布式锁 AOP 切面
 *
 * 学习要点:
 * 通过注解 + AOP 实现声明式分布式锁
 * 开发者只需 @DLock 注解，无需手动管理锁的获取和释放
 */
@Aspect
public class DistributedLockAspect {

    private static final Logger log = LoggerFactory.getLogger(DistributedLockAspect.class);

    private final DistributedLockFactory lockFactory;

    public DistributedLockAspect(DistributedLockFactory lockFactory) {
        this.lockFactory = lockFactory;
    }

    @Around("@annotation(dLock)")
    public Object around(ProceedingJoinPoint joinPoint, DLock dLock) throws Throwable {
        String lockKey = resolveKey(dLock.key(), joinPoint);
        DistributedLock lock = lockFactory.getLock(lockKey, Duration.ofSeconds(dLock.ttl()));

        boolean acquired = false;
        try {
            if (dLock.waitTime() > 0) {
                acquired = lock.tryLock(Duration.ofSeconds(dLock.waitTime()));
            } else {
                acquired = lock.tryLock();
            }

            if (!acquired) {
                throw new DistributedLockException(lockKey, dLock.message());
            }

            log.debug("获取锁成功: {}", lockKey);
            return joinPoint.proceed();
        } finally {
            if (acquired) {
                lock.unlock();
                log.debug("释放锁: {}", lockKey);
            }
        }
    }

    /**
     * 解析锁的 key - 简化版，直接使用方法签名 + 参数
     */
    private String resolveKey(String keyExpression, ProceedingJoinPoint joinPoint) {
        // 简化实现: 直接用注解的 key 值
        // 完整版应该解析 SpEL 表达式
        return keyExpression;
    }
}
