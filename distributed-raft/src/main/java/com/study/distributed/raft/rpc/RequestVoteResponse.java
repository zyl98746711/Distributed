package com.study.distributed.raft.rpc;

import java.io.Serializable;

/**
 * RequestVote RPC 响应
 *
 * - voteGranted: 是否授予投票
 * - term: 当前任期，用于候选人更新自己
 */
public record RequestVoteResponse(
        long term,
        boolean voteGranted
) implements Serializable {
}
