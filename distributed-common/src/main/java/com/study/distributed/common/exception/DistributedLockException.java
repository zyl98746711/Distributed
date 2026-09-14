package com.study.distributed.common.exception;

/**
 * 分布式锁异常
 */
public class DistributedLockException extends RuntimeException {

    public DistributedLockException(String message) {
        super(message);
    }

    public DistributedLockException(String lockKey, String reason) {
        super("分布式锁操作失败 [" + lockKey + "]: " + reason);
    }
}
