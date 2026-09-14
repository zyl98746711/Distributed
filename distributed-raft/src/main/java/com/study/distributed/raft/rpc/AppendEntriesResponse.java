package com.study.distributed.raft.rpc;

import java.io.Serializable;

/**
 * AppendEntries RPC 响应
 *
 * - success: 如果 Follower 成功匹配了 prevLogIndex 和 prevLogTerm
 *   Leader 根据此判断是否需要回退 nextIndex 重试
 */
public record AppendEntriesResponse(
        long term,
        boolean success,
        String responderId,
        long lastAppliedIndex
) implements Serializable {
}
