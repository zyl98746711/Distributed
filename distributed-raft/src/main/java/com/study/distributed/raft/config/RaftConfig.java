package com.study.distributed.raft.config;

/**
 * Raft 配置
 *
 * 学习要点:
 * - electionTimeoutMin/Max: 选举超时范围 (毫秒)
 *   随机化是关键! 避免多个 Candidate 同时发起选举导致分裂投票
 * - heartbeatInterval: 心跳间隔，应远小于选举超时
 * - 经验法则: heartbeatInterval < electionTimeoutMin < electionTimeoutMax
 */
public record RaftConfig(
        String nodeId,
        String host,
        int rpcPort,
        int electionTimeoutMin,
        int electionTimeoutMax,
        int heartbeatInterval
) {
    /**
     * 默认配置
     */
    public static RaftConfig of(String nodeId, String host, int rpcPort) {
        return new RaftConfig(nodeId, host, rpcPort, 3000, 5000, 1000);
    }

    /**
     * 自定义超时的配置
     */
    public static RaftConfig of(String nodeId, String host, int rpcPort,
                                 int electionTimeoutMin, int electionTimeoutMax, int heartbeatInterval) {
        return new RaftConfig(nodeId, host, rpcPort, electionTimeoutMin, electionTimeoutMax, heartbeatInterval);
    }
}
