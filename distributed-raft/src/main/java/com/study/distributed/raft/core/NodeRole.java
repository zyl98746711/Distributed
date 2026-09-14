package com.study.distributed.raft.core;

/**
 * Raft 节点角色状态
 *
 * 学习要点: Raft 中每个节点在任意时刻处于以下三种角色之一:
 * - Follower: 被动响应请求，等待 Leader 心跳
 * - Candidate: 发起选举，请求其他节点投票
 * - Leader: 处理客户端请求，发送心跳维持权威
 */
public enum NodeRole {
    FOLLOWER,
    CANDIDATE,
    LEADER
}
