package com.study.distributed.id.snowflake;

import com.study.distributed.id.api.IdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 雪花算法 (Snowflake) 实现
 *
 * ID 结构 (64 bit):
 * +------+----------------+----------+--------------+
 * | 符号 | 时间戳 (41bit) | 机器ID   | 序列号       |
 * | 0    | 41位毫秒级     | 10位     | 12位         |
 * +------+----------------+----------+--------------+
 *
 * 学习要点:
 * - 41bit 时间戳: 可用约 69 年 (2^41 毫秒)
 * - 10bit 机器ID: 最多 1024 个节点
 * - 12bit 序列号: 每毫秒每节点最多 4096 个 ID
 * - 时钟回拨是最大挑战
 */
public class SnowflakeIdGenerator implements IdGenerator {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeIdGenerator.class);

    // ====== 位分配 ======
    private static final long EPOCH = 1704067200000L; // 2024-01-01 00:00:00 UTC
    private static final long WORKER_ID_BITS = 10L;
    private static final long SEQUENCE_BITS = 12L;

    // ====== 最大值 ======
    private static final long MAX_WORKER_ID = ~(-1L << WORKER_ID_BITS); // 1023
    private static final long MAX_SEQUENCE = ~(-1L << SEQUENCE_BITS);    // 4095

    // ====== 位移 ======
    private static final long WORKER_ID_SHIFT = SEQUENCE_BITS;
    private static final long TIMESTAMP_SHIFT = SEQUENCE_BITS + WORKER_ID_BITS;

    // ====== 实例状态 ======
    private final long workerId;
    private long lastTimestamp = -1L;
    private long sequence = 0L;

    // ====== 时钟回拨配置 ======
    private static final long MAX_BACKWARD_MS = 5L;

    public SnowflakeIdGenerator(long workerId) {
        if (workerId < 0 || workerId > MAX_WORKER_ID) {
            throw new IllegalArgumentException("Worker ID 必须在 0-" + MAX_WORKER_ID + " 之间");
        }
        this.workerId = workerId;
    }

    @Override
    public synchronized long nextId() {
        long currentTimestamp = currentTime();

    /**
     * 时钟回拨处理
     *
     * 什么是时钟回拨?
     *   服务器的时钟可能通过 NTP 同步被回调。例如:
     *   10:00:05.000 → 10:00:04.500 (NTP 同步后回拨了 500ms)
     *
     * 为什么危险?
     *   如果回拨后生成的 ID 和之前的 ID 相同，就破坏了唯一性!
     *
     * 处理策略:
     *   小幅回拨 (≤5ms): 等待 2 倍时间让时钟追上
     *   大幅回拨 (>5ms): 直接抛异常，因为等待时间太长
     *
     * 生产环境更好的方案:
     *   - 使用单调递增时钟 (如 clock_gettime(CLOCK_MONOTONIC))
     *   - 记录上次生成时间到磁盘，重启时检查
     */
        if (currentTimestamp < lastTimestamp) {
            long offset = lastTimestamp - currentTimestamp;
            if (offset <= MAX_BACKWARD_MS) {
                // 小幅回拨: 等待追上
                try {
                    Thread.sleep(offset << 1); // 等待 2 倍时间
                    currentTimestamp = currentTime();
                    if (currentTimestamp < lastTimestamp) {
                        throw new RuntimeException("时钟回拨，等待后仍未恢复");
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("等待时钟恢复被中断", e);
                }
            } else {
                throw new RuntimeException("时钟回拨过大: " + offset + "ms");
            }
        }

        if (currentTimestamp == lastTimestamp) {
            // 同一毫秒: 递增序列号
            // & MAX_SEQUENCE 相当于 % 4096，但位运算更快
            // 当 sequence 达到 4095 时，+1 后 & 4095 = 0，自然溢出
            sequence = (sequence + 1) & MAX_SEQUENCE;
            if (sequence == 0) {
                // 当前毫秒序列号用完，等待下一毫秒
                currentTimestamp = waitNextMillis(lastTimestamp);
            }
        } else {
            // 新的毫秒: 序列号归零
            sequence = 0L;
        }

        lastTimestamp = currentTimestamp;

        // 组合 ID (位运算拼接)
        // 例如: timestamp=1000, workerId=5, sequence=3
        // 1000 << 22 = 4194304000  (时间戳部分)
        // 5 << 12    = 20480       (机器ID部分)
        // 3          = 3           (序列号部分)
        // 最终 ID    = 4194324483
        long id = ((currentTimestamp - EPOCH) << TIMESTAMP_SHIFT)
                | (workerId << WORKER_ID_SHIFT)
                | sequence;

        log.trace("生成 ID: {}, timestamp={}, workerId={}, sequence={}", id, currentTimestamp, workerId, sequence);
        return id;
    }

    private long waitNextMillis(long lastTs) {
        long ts = currentTime();
        while (ts <= lastTs) {
            ts = currentTime();
        }
        return ts;
    }

    private long currentTime() {
        return System.currentTimeMillis();
    }

    // ====== ID 解析 (调试用) ======

    public static long getTimestamp(long id) {
        return (id >> TIMESTAMP_SHIFT) + EPOCH;
    }

    public static long getWorkerId(long id) {
        return (id >> WORKER_ID_SHIFT) & MAX_WORKER_ID;
    }

    public static long getSequence(long id) {
        return id & MAX_SEQUENCE;
    }
}
