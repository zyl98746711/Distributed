package com.study.distributed.raft.rpc;

import java.io.Serializable;

/**
 * RequestVote RPC 请求
 *
 * 学习要点: Candidate 在选举时向其他节点发送此请求
 * - candidateId: 请求投票的候选人
 * - lastLogIndex/lastLogTerm: 候选人的最后一条日志信息
 *   用于 "日志完整性检查"：只有日志至少和投票人一样新的候选人才有资格获得投票
 */
public record RequestVoteRequest(
        long term,
        String candidateId,
        long lastLogIndex,
        long lastLogTerm
) implements Serializable {
}
