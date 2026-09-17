package com.study.distributed.node.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 节点配置 - 对应 application.yml 中的 raft.*
 */
@ConfigurationProperties(prefix = "raft")
public class RaftNodeProperties {

    /** 本节点 ID, 如 node-1 */
    private String nodeId;

    /** 本节点监听地址 */
    private String host = "127.0.0.1";

    /** 本节点 Raft 通信端口 (SocketRaftRpcService 监听) */
    private int rpcPort;

    /**
     * 集群节点列表, 格式: id@host:rpcPort:httpPort, 逗号分隔
     * 示例: node-1@127.0.0.1:9001:8001,node-2@127.0.0.1:9002:8002,node-3@127.0.0.1:9003:8003
     */
    private String peers;

    /** 选举超时下限 (毫秒) */
    private int electionTimeoutMin = 2000;

    /** 选举超时上限 (毫秒) */
    private int electionTimeoutMax = 3000;

    /** 心跳间隔 (毫秒) */
    private int heartbeatInterval = 500;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public String getPeers() {
        return peers;
    }

    public void setPeers(String peers) {
        this.peers = peers;
    }

    public int getElectionTimeoutMin() {
        return electionTimeoutMin;
    }

    public void setElectionTimeoutMin(int electionTimeoutMin) {
        this.electionTimeoutMin = electionTimeoutMin;
    }

    public int getElectionTimeoutMax() {
        return electionTimeoutMax;
    }

    public void setElectionTimeoutMax(int electionTimeoutMax) {
        this.electionTimeoutMax = electionTimeoutMax;
    }

    public int getHeartbeatInterval() {
        return heartbeatInterval;
    }

    public void setHeartbeatInterval(int heartbeatInterval) {
        this.heartbeatInterval = heartbeatInterval;
    }
}
