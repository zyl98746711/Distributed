package com.study.distributed.rpc.registry;

import com.study.distributed.common.model.NodeInfo;

import java.util.List;

/**
 * 服务注册表接口
 *
 * 学习要点: 服务注册与发现是微服务/分布式系统的基石
 * - 服务提供者启动时注册自己的地址
 * - 服务消费者通过服务名查找可用地址
 */
public interface ServiceRegistry {

    /**
     * 注册服务
     */
    void register(String serviceName, NodeInfo nodeInfo);

    /**
     * 注销服务
     */
    void unregister(String serviceName, NodeInfo nodeInfo);

    /**
     * 查找服务的所有节点
     */
    List<NodeInfo> lookup(String serviceName);
}
