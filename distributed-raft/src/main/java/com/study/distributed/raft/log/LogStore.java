package com.study.distributed.raft.log;

import java.util.List;

/**
 * 日志存储接口
 *
 * 学习要点: Raft 要求日志持久化，即使节点重启也不能丢失已提交的日志
 * 这里提供接口抽象，可以有内存实现和文件实现
 */
public interface LogStore {

    /**
     * 追加日志条目
     */
    void append(LogEntry entry);

    /**
     * 获取指定索引的日志
     */
    LogEntry get(long index);

    /**
     * 获取日志范围 [start, end]
     */
    List<LogEntry> getRange(long start, long end);

    /**
     * 获取最后一条日志的索引 (没有日志返回 0)
     */
    long lastIndex();

    /**
     * 获取最后一条日志的任期 (没有日志返回 0)
     */
    long lastTerm();

    /**
     * 删除从 index 开始的所有日志 (用于冲突截断)
     */
    void truncateFrom(long index);

    /**
     * 日志总数
     */
    long size();
}
