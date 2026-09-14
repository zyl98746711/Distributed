package com.study.distributed.lock.api;

import java.time.Duration;

/**
 * 分布式锁工厂
 */
public interface DistributedLockFactory {

    /**
     * 获取指定名称的锁
     */
    DistributedLock getLock(String lockName);

    /**
     * 获取指定名称的锁 (带 TTL)
     */
    DistributedLock getLock(String lockName, Duration ttl);
}
