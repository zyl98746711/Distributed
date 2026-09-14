package com.study.distributed.lock.impl;

import com.study.distributed.lock.api.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.LockSupport;

/**
 * 简单分布式锁 - 基于内存 + TTL
 *
 * 学习要点:
 * 这是最简单的实现，用于对比学习:
 * - 优点: 实现简单，无外部依赖
 * - 缺点: 单点故障、无法跨节点、非真正分布式
 *
 * 对比 RaftDistributedLock 理解分布式锁的演进
 */
public class SimpleDistributedLock implements DistributedLock {

    private static final Logger log = LoggerFactory.getLogger(SimpleDistributedLock.class);

    /** 全局锁表: lockName -> LockInfo */
    private static final Map<String, LockInfo> LOCKS = new ConcurrentHashMap<>();

    private final String lockName;
    private final Duration ttl;
    private final String ownerId;

    /** 重入计数 */
    private int holdCount = 0;

    public SimpleDistributedLock(String lockName, Duration ttl) {
        this.lockName = lockName;
        this.ttl = ttl;
        this.ownerId = Thread.currentThread().threadId() + "-" + System.nanoTime();
    }

    @Override
    public void lock() {
        while (!tryLock()) {
            LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
        }
    }

    @Override
    public boolean tryLock() {
        // 可重入检查
        LockInfo existing = LOCKS.get(lockName);
        if (existing != null && existing.ownerId.equals(currentOwnerId())) {
            holdCount++;
            return true;
        }

        // 检查是否过期
        if (existing != null && existing.isExpired()) {
            LOCKS.remove(lockName);
            log.info("锁 {} 已过期，强制释放", lockName);
        }

        // 尝试获取
        LockInfo newLock = new LockInfo(currentOwnerId(), System.currentTimeMillis(), ttl.toMillis());
        LockInfo prev = LOCKS.putIfAbsent(lockName, newLock);

        if (prev == null || prev.isExpired()) {
            LOCKS.put(lockName, newLock);
            holdCount = 1;
            log.debug("获取锁: {}", lockName);
            return true;
        }

        return false;
    }

    @Override
    public boolean tryLock(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (tryLock()) return true;
            LockSupport.parkNanos(Duration.ofMillis(50).toNanos());
        }
        return false;
    }

    @Override
    public void unlock() {
        if (!isHeldByCurrentThread()) {
            throw new IllegalMonitorStateException("不是锁的持有者");
        }
        holdCount--;
        if (holdCount == 0) {
            LOCKS.remove(lockName);
            log.debug("释放锁: {}", lockName);
        }
    }

    @Override
    public boolean isHeldByCurrentThread() {
        LockInfo info = LOCKS.get(lockName);
        return info != null && info.ownerId.equals(currentOwnerId());
    }

    private String currentOwnerId() {
        return ownerId;
    }

    /**
     * 锁信息
     */
    private record LockInfo(String ownerId, long acquireTime, long ttlMs) {
        boolean isExpired() {
            return System.currentTimeMillis() - acquireTime > ttlMs;
        }
    }
}
