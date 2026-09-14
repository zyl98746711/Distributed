package com.study.distributed.rpc.protocol;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RPC 请求 - 使用 Record 定义
 *
 * 包含服务名、方法名、参数类型、参数值等信息
 */
public record RpcRequest(
        String requestId,
        String serviceName,
        String methodName,
        String[] paramTypes,
        Object[] params
) implements Serializable {

    private static final AtomicLong ID_GEN = new AtomicLong(0);

    public static String nextId() {
        return "req-" + ID_GEN.incrementAndGet();
    }
}
