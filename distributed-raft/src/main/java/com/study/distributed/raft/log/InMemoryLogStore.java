package com.study.distributed.raft.log;

import java.util.ArrayList;
import java.util.List;

/**
 * 内存日志存储 - 教学用实现
 *
 * 学习要点: 生产环境需要 WAL (Write-Ahead Log) 持久化到磁盘
 * 这里用 ArrayList 模拟，便于理解 Raft 算法逻辑
 */
public class InMemoryLogStore implements LogStore {

    /** 日志列表，index 0 是占位符 (Raft 日志从 1 开始) */
    private final List<LogEntry> logs = new ArrayList<>();

    public InMemoryLogStore() {
        // 索引 0 放一个占位条目
        logs.add(new LogEntry(0, 0, null));
    }

    @Override
    public void append(LogEntry entry) {
        logs.add(entry);
    }

    @Override
    public LogEntry get(long index) {
        if (index < 1 || index >= logs.size()) return null;
        return logs.get((int) index);
    }

    @Override
    public List<LogEntry> getRange(long start, long end) {
        int from = (int) Math.max(1, start);
        int to = (int) Math.min(logs.size() - 1, end);
        if (from > to) return List.of();
        return new ArrayList<>(logs.subList(from, to + 1));
    }

    @Override
    public long lastIndex() {
        return logs.size() - 1;
    }

    @Override
    public long lastTerm() {
        if (logs.size() <= 1) return 0;
        return logs.getLast().term();
    }

    @Override
    public void truncateFrom(long index) {
        if (index < 1 || index >= logs.size()) return;
        // 删除 [index, size) 的所有条目
        while (logs.size() > index) {
            logs.removeLast();
        }
    }

    @Override
    public long size() {
        return logs.size() - 1; // 去掉占位符
    }
}
