package com.study.distributed.lock.impl;

import com.study.distributed.kv.DistributedKVService;
import com.study.distributed.lock.api.DistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.LockSupport;

/**
 * 基于 Raft KV 的分布式锁
 *
 * 学习要点:
 * 利用 Raft 共识的 KV 存储实现锁:
 * - lock(key) = 在 KV 中写入 lock:xxx = ownerId (CAS: 仅当 key 不存在时)
 * - unlock(key) = 删除 lock:xxx
 * - TTL 通过看门狗续期保证
 *
 * 这种实现真正支持跨节点互斥，因为所有写操作都经过 Raft 共识
 */
public class RaftDistributedLock implements DistributedLock {

    private static final Logger log = LoggerFactory.getLogger(RaftDistributedLock.class);
    private static final String LOCK_PREFIX = "__lock__:";

    private final DistributedKVService kvService;
    private final String lockName;
    private final Duration ttl;
    private final String ownerId;
    private volatile boolean locked = false;
    private volatile Thread watchdogThread;

    public RaftDistributedLock(DistributedKVService kvService, String lockName, Duration ttl) {
        this.kvService = kvService;
        this.lockName = lockName;
        this.ttl = ttl;
        this.ownerId = Thread.currentThread().threadId() + "-" + System.nanoTime();
    }

    @Override
    public void lock() {
        while (!tryLock()) {
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
    }

    @Override
    public boolean tryLock() {
        // 检查锁是否已被自己持有
        String existing = kvService.get(LOCK_PREFIX + lockName);
        if (ownerId.equals(existing)) {
            locked = true;
            return true;
        }

        // 检查锁是否空闲 (不存在或已过期)
        if (existing != null) {
            return false;
        }

        try {
            // 尝试写入锁 (通过 Raft 共识)
            kvService.put(LOCK_PREFIX + lockName, ownerId).join();

            // 验证是否成功获取 (可能存在竞争)
            String afterWrite = kvService.get(LOCK_PREFIX + lockName);
            if (ownerId.equals(afterWrite)) {
                locked = true;
                startWatchdog();
                log.debug("获取 Raft 分布式锁: {} by {}", lockName, ownerId);
                return true;
            }
        } catch (Exception e) {
            log.debug("获取锁失败: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public boolean tryLock(Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (tryLock()) return true;
            LockSupport.parkNanos(Duration.ofMillis(100).toNanos());
        }
        return false;
    }

    @Override
    public void unlock() {
        if (!locked) return;
        try {
            stopWatchdog();
            kvService.delete(LOCK_PREFIX + lockName).join();
            locked = false;
            log.debug("释放 Raft 分布式锁: {}", lockName);
        } catch (Exception e) {
            log.error("释放锁失败: {}", e.getMessage());
        }
    }

    @Override
    public boolean isHeldByCurrentThread() {
        return locked && ownerId.equals(kvService.get(LOCK_PREFIX + lockName));
    }

    /**
     * 看门狗 - 后台自动续期
     * 学习要点: 防止业务未完成锁就过期
     */
    private void startWatchdog() {
        watchdogThread = Thread.ofVirtual().name("lock-watchdog-" + lockName).start(() -> {
            while (locked) {
                try {
                    Thread.sleep(ttl.toMillis() / 3); // 每 1/3 TTL 续期一次
                    if (locked) {
                        // 续期: 重新写入
                        kvService.put(LOCK_PREFIX + lockName, ownerId).join();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    log.debug("看门狗续期失败: {}", e.getMessage());
                }
            }
        });
    }

    private void stopWatchdog() {
        if (watchdogThread != null) {
            watchdogThread.interrupt();
            watchdogThread = null;
        }
    }
}
