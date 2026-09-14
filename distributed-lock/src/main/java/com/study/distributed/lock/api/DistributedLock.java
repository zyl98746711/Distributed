package com.study.distributed.lock.api;

import java.time.Duration;

/**
 * 分布式锁接口
 *
 * 学习要点:
 * 分布式锁需要解决的核心问题:
 * 1. 互斥: 同一时刻只有一个持有者
 * 2. 容错: 节点故障后锁能自动释放 (TTL)
 * 3. 可重入: 同一线程可重复获取同一把锁
 * 4. 公平性: 可选的公平/非公平策略
 */
public interface DistributedLock {

    /**
     * 加锁 (阻塞直到获取)
     */
    void lock();

    /**
     * 尝试加锁
     * @return true=获取成功, false=获取失败
     */
    boolean tryLock();

    /**
     * 尝试加锁 (带超时)
     */
    boolean tryLock(Duration timeout);

    /**
     * 解锁
     */
    void unlock();

    /**
     * 是否被当前线程持有
     */
    boolean isHeldByCurrentThread();
}
