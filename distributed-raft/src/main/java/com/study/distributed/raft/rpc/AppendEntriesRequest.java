package com.study.distributed.raft.rpc;

import com.study.distributed.raft.log.LogEntry;

import java.io.Serializable;
import java.util.List;

/**
 * AppendEntries RPC 请求
 *
 * 学习要点: Leader 通过此 RPC 复制日志给 Follower，也用作心跳
 * - prevLogIndex/prevLogTerm: 新日志条目之前的那条日志的索引和任期
 *   用于 "日志匹配"：Follower 检查自己是否有匹配的日志，保证一致性
 * - entries: 要追加的日志条目 (空数组 = 心跳)
 * - leaderCommit: Leader 的提交索引，Follower 据此更新自己的提交点
 */
public record AppendEntriesRequest(
        long term,
        String leaderId,
        long prevLogIndex,
        long prevLogTerm,
        List<LogEntry> entries,
        long leaderCommit
) implements Serializable {
}
