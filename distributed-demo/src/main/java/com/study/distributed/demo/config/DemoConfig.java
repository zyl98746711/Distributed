package com.study.distributed.demo.config;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.id.api.IdGenerator;
import com.study.distributed.id.snowflake.SnowflakeIdGenerator;
import com.study.distributed.kv.DistributedKVService;
import com.study.distributed.kv.statemachine.KVStateMachine;
import com.study.distributed.raft.config.RaftConfig;
import com.study.distributed.raft.core.RaftNode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

/**
 * 演示配置 - 3 节点 Raft 集群 (同一 JVM 内)
 *
 * 架构:
 * <pre>
 * ┌─────────────────────────────────────────────────────────┐
 * │                      JVM 进程                            │
 * │                                                         │
 * │  ┌──────────┐  ┌──────────┐  ┌──────────┐             │
 * │  │  node-1  │  │  node-2  │  │  node-3  │             │
 * │  │ RaftNode │  │ RaftNode │  │ RaftNode │             │
 * │  │ KV SM    │  │ KV SM    │  │ KV SM    │             │
 * │  └────┬─────┘  └────┬─────┘  └────┬─────┘             │
 * │       │              │              │                   │
 * │       └──────────┬───┴──────────────┘                   │
 * │                  │                                      │
 * │         InMemoryRaftRpcService                          │
 * │         (共享节点注册表)                                  │
 * │                                                         │
 * │  ┌──────────────────────────────────────────┐          │
 * │  │  HTTP API (Spring Boot :8080)            │          │
 * │  │  /kv/node-1/put?key=x&value=y            │          │
 * │  │  /kv/node-2/get?key=x                    │          │
 * │  │  /cluster/status                         │          │
 * │  └──────────────────────────────────────────┘          │
 * └─────────────────────────────────────────────────────────┘
 * </pre>
 *
 * 学习要点:
 * - 3 个节点各自维护独立的状态机 (KVStateMachine)
 * - 通过 InMemoryRaftRpcService 模拟网络通信
 * - 启动后会自动选举 Leader，日志中可观察选举过程
 * - 写操作必须发给 Leader，读操作可以发给任意节点
 */
@Configuration
public class DemoConfig {

    /** 集群节点列表 (共享给所有节点) */
    private static final List<NodeInfo> CLUSTER_PEERS = List.of(
            new NodeInfo("node-1", "127.0.0.1", 9001),
            new NodeInfo("node-2", "127.0.0.1", 9002),
            new NodeInfo("node-3", "127.0.0.1", 9003)
    );

    /**
     * 创建 3 个 Raft 节点 + 各自的 KV 状态机
     * 所有节点共享同一个 InMemoryRaftRpcService 注册表
     */
    @Bean
    public RaftCluster raftCluster() {
        RaftCluster cluster = new RaftCluster();

        // 创建 3 个节点，每个节点有独立的状态机和 RPC 服务
        for (int i = 0; i < 3; i++) {
            NodeInfo peer = CLUSTER_PEERS.get(i);
            String nodeId = peer.id();

            // 每个节点独立配置，缩短超时便于快速观察选举
            RaftConfig config = RaftConfig.of(
                    nodeId, peer.host(), peer.port(),
                    1500 + i * 200,   // 不同的选举超时: 1500, 1700, 1900
                    2500 + i * 200,   // 不同的选举超时: 2500, 2700, 2900
                    500               // 统一心跳间隔
            );

            // 每个节点独立的状态机 (各自的 KV 存储)
            KVStateMachine stateMachine = new KVStateMachine();

            // 内存 RPC 服务 (共享注册表)
            InMemoryRaftRpcService rpcService = new InMemoryRaftRpcService();

            // 创建 Raft 节点
            RaftNode node = new RaftNode(config, CLUSTER_PEERS, stateMachine, rpcService);

            // 注册到集群
            cluster.addNode(nodeId, node, stateMachine);
            InMemoryRaftRpcService.registerNode(node);
        }

        // 启动所有节点
        cluster.startAll();

        return cluster;
    }

    @Bean
    public IdGenerator idGenerator() {
        return new SnowflakeIdGenerator(1);
    }

    /**
     * Raft 集群 - 管理同一 JVM 内的多个 Raft 节点
     */
    public static class RaftCluster {

        private final Map<String, RaftNode> nodes = new java.util.LinkedHashMap<>();
        private final Map<String, KVStateMachine> stateMachines = new java.util.LinkedHashMap<>();
        private final Map<String, DistributedKVService> kvServices = new java.util.LinkedHashMap<>();

        public void addNode(String nodeId, RaftNode node, KVStateMachine stateMachine) {
            nodes.put(nodeId, node);
            stateMachines.put(nodeId, stateMachine);
            kvServices.put(nodeId, new DistributedKVService(node, stateMachine));
        }

        public void startAll() {
            nodes.values().forEach(RaftNode::start);
        }

        public RaftNode getNode(String nodeId) {
            return nodes.get(nodeId);
        }

        public DistributedKVService getKvService(String nodeId) {
            return kvServices.get(nodeId);
        }

        public Map<String, RaftNode> getAllNodes() {
            return nodes;
        }

        public Map<String, DistributedKVService> getAllKvServices() {
            return kvServices;
        }

        /**
         * 获取 Leader 节点 ID
         */
        public String getLeaderId() {
            return nodes.values().stream()
                    .filter(n -> n.getRole() == com.study.distributed.raft.core.NodeRole.LEADER)
                    .map(RaftNode::getNodeId)
                    .findFirst()
                    .orElse(null);
        }

        /**
         * 获取集群状态概览
         */
        public Map<String, Object> getClusterStatus() {
            Map<String, Object> status = new java.util.LinkedHashMap<>();
            status.put("leader", getLeaderId());

            Map<String, Object> nodeStatuses = new java.util.LinkedHashMap<>();
            for (var entry : nodes.entrySet()) {
                RaftNode node = entry.getValue();
                nodeStatuses.put(entry.getKey(), Map.of(
                        "role", node.getRole().name(),
                        "term", node.getCurrentTerm(),
                        "commitIndex", node.getCommitIndex(),
                        "lastApplied", node.getLastApplied(),
                        "logSize", node.getLogStore().size()
                ));
            }
            status.put("nodes", nodeStatuses);
            return status;
        }
    }
}
