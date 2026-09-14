package com.study.distributed.rpc.registry;

import com.study.distributed.common.model.NodeInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 内存服务注册表 - 最简单的实现
 *
 * 学习要点: 这是最朴素的注册中心实现
 * 生产环境需要: 持久化、健康检查、负载均衡等
 */
public class InMemoryServiceRegistry implements ServiceRegistry {

    /** 服务名 -> 节点列表 */
    private final Map<String, List<NodeInfo>> registry = new ConcurrentHashMap<>();

    @Override
    public void register(String serviceName, NodeInfo nodeInfo) {
        registry.computeIfAbsent(serviceName, k -> new ArrayList<>()).add(nodeInfo);
    }

    @Override
    public void unregister(String serviceName, NodeInfo nodeInfo) {
        registry.computeIfPresent(serviceName, (k, nodes) -> {
            nodes.removeIf(n -> n.id().equals(nodeInfo.id()));
            return nodes.isEmpty() ? null : nodes;
        });
    }

    @Override
    public List<NodeInfo> lookup(String serviceName) {
        return registry.getOrDefault(serviceName, List.of());
    }
}
