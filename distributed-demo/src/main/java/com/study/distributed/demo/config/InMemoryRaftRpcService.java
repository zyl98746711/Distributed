package com.study.distributed.demo.config;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.raft.core.RaftNode;
import com.study.distributed.raft.core.RaftRpcService;
import com.study.distributed.raft.rpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存 RPC 服务 - 同一 JVM 内多个 Raft 节点直接通信
 *
 * 学习要点:
 * 真实环境中，节点间通过网络 (Socket) 通信。
 * 这里为了在单进程内演示 3 节点集群，
 * 使用共享注册表让节点直接调用对方的方法，跳过网络层。
 *
 * 这相当于一个 "进程内网络模拟器"，方便观察 Raft 算法行为。
 */
public class InMemoryRaftRpcService implements RaftRpcService {

    private static final Logger log = LoggerFactory.getLogger(InMemoryRaftRpcService.class);

    /** 全局节点注册表: nodeId -> RaftNode (所有节点共享) */
    private static final Map<String, RaftNode> NODE_REGISTRY = new ConcurrentHashMap<>();

    /** 注册节点到全局注册表 */
    public static void registerNode(RaftNode node) {
        NODE_REGISTRY.put(node.getNodeId(), node);
        log.info("注册 Raft 节点: {}", node.getNodeId());
    }

    /** 获取注册表 (用于调试) */
    public static Map<String, RaftNode> getNodeRegistry() {
        return NODE_REGISTRY;
    }

    @Override
    public RequestVoteResponse requestVote(NodeInfo target, RequestVoteRequest request) throws Exception {
        RaftNode targetNode = NODE_REGISTRY.get(target.id());
        if (targetNode == null) {
            throw new RuntimeException("目标节点不存在: " + target.id());
        }

        log.debug("[{}] → [{}] RequestVote(term={})",
                request.candidateId(), target.id(), request.term());

        // 直接调用目标节点的 handleRequestVote
        return targetNode.handleRequestVote(request);
    }

    @Override
    public AppendEntriesResponse appendEntries(NodeInfo target, AppendEntriesRequest request) throws Exception {
        RaftNode targetNode = NODE_REGISTRY.get(target.id());
        if (targetNode == null) {
            throw new RuntimeException("目标节点不存在: " + target.id());
        }

        boolean isHeartbeat = request.entries().isEmpty();
        if (!isHeartbeat) {
            log.debug("[{}] → [{}] AppendEntries(term={}, entries={})",
                    request.leaderId(), target.id(), request.term(), request.entries().size());
        }

        // 直接调用目标节点的 handleAppendEntries
        return targetNode.handleAppendEntries(request);
    }
}
