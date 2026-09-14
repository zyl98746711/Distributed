package com.study.distributed.raft.log;

import java.io.Serializable;

/**
 * Raft 日志条目 - 使用 Record 定义
 *
 * 学习要点:
 * - term: 该条目被创建时的任期，用于检测不一致
 * - index: 日志中的位置，从 1 开始
 * - command: 要应用到状态机的命令 (序列化后的字节)
 */
public record LogEntry(
        long term,
        long index,
        byte[] command
) implements Serializable {
}
