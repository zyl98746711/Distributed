package com.study.distributed.rpc.protocol;

import java.io.Serializable;

/**
 * RPC 响应 - 使用 Record 定义
 */
public record RpcResponse(
        String requestId,
        Object result,
        String error,
        boolean success
) implements Serializable {

    public static RpcResponse ok(String requestId, Object result) {
        return new RpcResponse(requestId, result, null, true);
    }

    public static RpcResponse fail(String requestId, String error) {
        return new RpcResponse(requestId, null, error, false);
    }
}
