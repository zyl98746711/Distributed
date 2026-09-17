package com.study.distributed.node.config;

import com.study.distributed.common.model.NodeInfo;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 集群节点注册表
 *
 * 提供两类信息:
 * - Raft 节点间通信使用: List&lt;NodeInfo&gt; (host + rpcPort)
 * - Leader 转发使用: 按 nodeId 查到 PeerSpec (含 httpPort)
 */
public class PeerRegistry {

    private final List<PeerSpec> peers;
    private final Map<String, PeerSpec> byId;

    public PeerRegistry(List<PeerSpec> peers) {
        this.peers = List.copyOf(peers);
        Map<String, PeerSpec> map = new LinkedHashMap<>();
        for (PeerSpec peer : peers) {
            map.put(peer.id(), peer);
        }
        this.byId = Map.copyOf(map);
    }

    /** Raft 节点间通信使用的节点信息列表 */
    public List<NodeInfo> nodes() {
        return peers.stream().map(PeerSpec::toNodeInfo).toList();
    }

    /** 按 ID 查找节点 (用于 Leader 转发), 不存在返回 null */
    public PeerSpec byId(String nodeId) {
        return nodeId == null ? null : byId.get(nodeId);
    }

    /** 所有节点 */
    public Collection<PeerSpec> all() {
        return peers;
    }
}
