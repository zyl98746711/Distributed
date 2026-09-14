package com.study.distributed.id.segment;

import com.study.distributed.id.api.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * 号段模式 ID 生成器
 *
 * 学习要点:
 * - 批量获取 ID 号段 (如一次获取 1000 个)，减少对外部存储的访问
 * - 双 Buffer 预加载: 当前号段用到 20% 时，异步加载下一个号段
 * - 即使外部存储不可用，也能用本地缓存继续生成一段时间
 *
 * 这里用 LongSupplier 模拟号段获取 (实际可以对接 DB/文件/Raft KV)
 */
public class SegmentIdGenerator implements IdGenerator {

    private static final Logger log = LoggerFactory.getLogger(SegmentIdGenerator.class);

    private final int batchSize;
    private final LongSupplier maxIdSupplier;

    private volatile Segment currentSegment;
    private volatile Segment nextSegment;
    private volatile boolean loadingNext = false;

    /**
     * @param batchSize 每次获取的号段大小
     * @param maxIdSupplier 获取当前最大 ID 的函数 (返回下一个号段的起始值)
     */
    public SegmentIdGenerator(int batchSize, LongSupplier maxIdSupplier) {
        this.batchSize = batchSize;
        this.maxIdSupplier = maxIdSupplier;
        loadCurrentSegment();
    }

    @Override
    public long nextId() {
        long id = currentSegment.nextId();
        if (id == -1) {
            // 当前号段用完
            synchronized (this) {
                id = currentSegment.nextId();
                if (id == -1) {
                    // 切换到下一个号段
                    if (nextSegment != null) {
                        currentSegment = nextSegment;
                        nextSegment = null;
                    } else {
                        loadCurrentSegment();
                    }
                    id = currentSegment.nextId();
                }
            }
        }

        // 检查是否需要预加载下一个号段
        if (currentSegment.getUsagePercent() >= 0.2 && !loadingNext && nextSegment == null) {
            triggerLoadNext();
        }

        return id;
    }

    private void loadCurrentSegment() {
        long maxId = maxIdSupplier.getAsLong();
        currentSegment = new Segment(maxId, maxId + batchSize);
        log.info("加载号段: [{}, {})", maxId, maxId + batchSize);
    }

    private void triggerLoadNext() {
        loadingNext = true;
        Thread.ofVirtual().name("segment-loader").start(() -> {
            try {
                long maxId = maxIdSupplier.getAsLong();
                nextSegment = new Segment(maxId, maxId + batchSize);
                log.info("预加载号段: [{}, {})", maxId, maxId + batchSize);
            } finally {
                loadingNext = false;
            }
        });
    }

    /**
     * 号段 - 线程安全的 ID 分配器
     */
    private static class Segment {
        private final long start;
        private final long end;
        private final AtomicLong current;

        Segment(long start, long end) {
            this.start = start;
            this.end = end;
            this.current = new AtomicLong(start);
        }

        long nextId() {
            long val = current.getAndIncrement();
            return val < end ? val : -1;
        }

        double getUsagePercent() {
            return (double) (current.get() - start) / (end - start);
        }
    }
}
