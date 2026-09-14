package com.study.distributed.rpc.transport;

import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.common.serializer.Serializer;
import com.study.distributed.rpc.protocol.RpcProtocol;
import com.study.distributed.rpc.protocol.RpcRequest;
import com.study.distributed.rpc.protocol.RpcResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RPC 客户端 - 基于 Socket + Virtual Threads
 *
 * 学习要点:
 * 1. 一个连接上可能有多个请求在飞行，通过 requestId 匹配响应
 * 2. Virtual Threads 让每个请求可以阻塞等待响应而不占用平台线程
 * 3. 连接管理: 简化版，每个目标地址一个连接
 */
public class RpcClient {

    private static final Logger log = LoggerFactory.getLogger(RpcClient.class);

    private final Serializer serializer = new JsonSerializer();
    /** 待响应映射: requestId -> Future */
    private final Map<String, CompletableFuture<RpcResponse>> pendingRequests = new ConcurrentHashMap<>();
    /** 连接缓存: address -> Socket */
    private final Map<String, Socket> connections = new ConcurrentHashMap<>();

    /**
     * 发送 RPC 请求并等待响应
     */
    public RpcResponse send(String host, int port, RpcRequest request) throws Exception {
        Socket socket = getOrCreateConnection(host, port);
        CompletableFuture<RpcResponse> future = new CompletableFuture<>();
        pendingRequests.put(request.requestId(), future);

        try {
            // 编码并发送
            byte[] frame = RpcProtocol.encodeRequest(request, serializer);
            OutputStream out = socket.getOutputStream();
            synchronized (out) {
                out.write(frame);
                out.flush();
            }

            // 等待响应 (这里会阻塞当前 Virtual Thread)
            return future.get();
        } catch (Exception e) {
            pendingRequests.remove(request.requestId());
            throw e;
        }
    }

    /**
     * 异步发送
     */
    public CompletableFuture<RpcResponse> sendAsync(String host, int port, RpcRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return send(host, port, request);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * 启动响应读取线程 (每个连接一个 Virtual Thread)
     */
    private void startResponseReader(Socket socket, InputStream in) {
        Thread.ofVirtual().name("rpc-reader-" + socket.getRemoteSocketAddress()).start(() -> {
            try {
                while (!socket.isClosed()) {
                    RpcProtocol.Frame frame = RpcProtocol.decode(in);
                    RpcResponse response = serializer.deserialize(frame.data(), RpcResponse.class);
                    CompletableFuture<RpcResponse> future = pendingRequests.remove(response.requestId());
                    if (future != null) {
                        future.complete(response);
                    } else {
                        log.warn("收到未知请求ID的响应: {}", response.requestId());
                    }
                }
            } catch (Exception e) {
                if (!socket.isClosed()) {
                    log.error("读取响应异常: {}", e.getMessage());
                    // 失败所有待处理请求
                    pendingRequests.values().forEach(f -> f.completeExceptionally(e));
                    pendingRequests.clear();
                }
            }
        });
    }

    private Socket getOrCreateConnection(String host, int port) throws IOException {
        String address = host + ":" + port;
        return connections.computeIfAbsent(address, addr -> {
            try {
                Socket socket = new Socket(host, port);
                startResponseReader(socket, socket.getInputStream());
                log.info("创建 RPC 连接到 {}", address);
                return socket;
            } catch (IOException e) {
                throw new RuntimeException("连接失败: " + address, e);
            }
        });
    }

    public void close() {
        connections.values().forEach(socket -> {
            try { socket.close(); } catch (IOException ignored) {}
        });
        connections.clear();
    }
}
