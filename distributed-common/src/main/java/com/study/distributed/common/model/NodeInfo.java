package com.study.distributed.common.model;

import java.io.Serializable;

/**
 * 节点信息 - 表示集群中的一个节点
 */
public record NodeInfo(
        String id,
        String host,
        int port
) implements Serializable {

    public String address() {
        return host + ":" + port;
    }
}
