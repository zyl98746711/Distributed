package com.study.distributed.node.config;

import com.study.distributed.common.model.NodeInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * 集群节点描述 - 解析 peers 配置项
 *
 * 格式: id@host:rpcPort:httpPort
 * 示例: node-1@127.0.0.1:9001:8001
 *   - 9001: Raft 节点间通信端口 (SocketRaftRpcService 监听)
 *   - 8001: HTTP 管理端口 (非 Leader 写入时转发到 Leader 使用)
 */
public record PeerSpec(String id, String host, int rpcPort, int httpPort) {

    /** 转为 Raft 模块使用的节点信息 (只含 RPC 地址) */
    public NodeInfo toNodeInfo() {
        return new NodeInfo(id, host, rpcPort);
    }

    /**
     * 解析完整的 peers 字符串 (逗号分隔)
     */
    public static List<PeerSpec> parse(String peers) {
        if (peers == null || peers.isBlank()) {
            throw new IllegalArgumentException("raft.peers 配置不能为空");
        }
        List<PeerSpec> result = new ArrayList<>();
        for (String item : peers.split(",")) {
            String trimmed = item.trim();
            if (!trimmed.isEmpty()) {
                result.add(parseOne(trimmed));
            }
        }
        return result;
    }

    /**
     * 解析单个节点: id@host:rpcPort:httpPort
     */
    private static PeerSpec parseOne(String spec) {
        int at = spec.indexOf('@');
        if (at <= 0) {
            throw new IllegalArgumentException("peers 格式错误 (缺少 id@): " + spec);
        }
        String id = spec.substring(0, at).trim();
        String[] parts = spec.substring(at + 1).split(":");
        if (parts.length != 3) {
            throw new IllegalArgumentException("peers 格式错误 (应为 id@host:rpcPort:httpPort): " + spec);
        }
        try {
            return new PeerSpec(
                    id,
                    parts[0].trim(),
                    Integer.parseInt(parts[1].trim()),
                    Integer.parseInt(parts[2].trim()));
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("peers 端口格式错误: " + spec, e);
        }
    }
}
