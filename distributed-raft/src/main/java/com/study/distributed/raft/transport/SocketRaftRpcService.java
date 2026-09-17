package com.study.distributed.raft.transport;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.raft.core.RaftNode;
import com.study.distributed.raft.core.RaftRpcService;
import com.study.distributed.raft.rpc.AppendEntriesRequest;
import com.study.distributed.raft.rpc.AppendEntriesResponse;
import com.study.distributed.raft.rpc.RequestVoteRequest;
import com.study.distributed.raft.rpc.RequestVoteResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 基于 Socket 的 Raft RPC 服务 - 真实网络实现
 *
 * <p>与 demo 模块 InMemoryRaftRpcService(进程内直接调方法)的最大区别:</p>
 * <ul>
 *   <li>节点间通过真实 TCP 通信, 每个节点可以独立 JVM 进程部署</li>
 *   <li>可以真实地 kill 掉节点, 观察网络超时 / 重新选举 / 日志追赶</li>
 *   <li>消息经过序列化与网络传输, 是有成本的 (这正是分布式系统要面对的)</li>
 * </ul>
 *
 * <h3>线程模型 (Virtual Threads)</h3>
 * <pre>
 * 入站:
 *   raft-rpc-acceptor    循环 accept 新连接
 *   raft-rpc-handler-*   每个入站连接一个虚拟线程: 读请求 -> 交给 RaftNode 处理 -> 写响应
 *
 * 出站:
 *   raft-rpc-reader-*    每个出站连接一个虚拟线程: 持续读取响应, 按 requestId 匹配等待中的请求
 *   (调用方虚拟线程)      发送请求后阻塞等待响应 (带超时)
 * </pre>
 *
 * <h3>可靠性处理</h3>
 * <ul>
 *   <li>等待响应带超时 (默认 2000ms), 超时后清理连接缓存, 下次发送自动重连</li>
 *   <li>连接断开时, 快速失败该连接上所有等待中的请求</li>
 *   <li>目标节点被 kill 后, 发送会抛异常, 由 RaftNode 捕获并容忍 (日志 debug),
 *       这正是 Raft 能在故障中继续工作的前提</li>
 * </ul>
 */
public class SocketRaftRpcService implements RaftRpcService {

    private static final Logger log = LoggerFactory.getLogger(SocketRaftRpcService.class);

    /** 默认 RPC 超时 (毫秒) */
    public static final long DEFAULT_RPC_TIMEOUT_MS = 2000;

    /** 本节点 ID (仅用于日志与 requestId 生成) */
    private final String nodeId;

    /** 本节点监听的 RPC 端口 */
    private final int listenPort;

    /** RPC 调用超时 (毫秒) */
    private final long rpcTimeoutMs;

    /** 本地节点 (入站请求的处理目标), bind() 时注入 */
    private volatile RaftNode localNode;

    /** 入站服务器运行标记 */
    private volatile boolean running = false;
    private ServerSocket serverSocket;

    /** 出站连接缓存: address(host:port) -> Socket */
    private final Map<String, Socket> connections = new ConcurrentHashMap<>();

    /** 出站等待响应的请求: requestId -> PendingCall */
    private final Map<String, PendingCall> pendingResponses = new ConcurrentHashMap<>();

    public SocketRaftRpcService(String nodeId, int listenPort) {
        this(nodeId, listenPort, DEFAULT_RPC_TIMEOUT_MS);
    }

    public SocketRaftRpcService(String nodeId, int listenPort, long rpcTimeoutMs) {
        this.nodeId = nodeId;
        this.listenPort = listenPort;
        this.rpcTimeoutMs = rpcTimeoutMs;
    }

    /**
     * 绑定本地 Raft 节点
     *
     * 为什么需要 bind? 存在循环依赖:
     *   节点需要 rpcService 发消息, rpcService 又需要节点来处理收到的消息。
     * demo 的 InMemory 实现用静态注册表绕过, 这里用显式 bind 更清晰。
     */
    public void bind(RaftNode node) {
        this.localNode = node;
    }

    /**
     * 启动入站服务 (监听端口, 接收其他节点的 RPC 请求)
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(listenPort);
        running = true;
        log.info("[{}] Raft RPC 服务启动, 监听端口 {}", nodeId, listenPort);

        Thread.ofVirtual().name("raft-rpc-acceptor-" + nodeId).start(() -> {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    socket.setTcpNoDelay(true);
                    Thread.ofVirtual().name("raft-rpc-handler-" + nodeId + "-" + socket.getRemoteSocketAddress())
                            .start(() -> handleInbound(socket));
                } catch (IOException e) {
                    if (running) {
                        log.error("[{}] 接受连接异常: {}", nodeId, e.getMessage());
                    }
                }
            }
        });
    }

    // ================================================================
    //  入站: 处理其他节点发来的请求
    // ================================================================

    /**
     * 处理一个入站连接: 循环 读请求 -> 分发处理 -> 写响应
     */
    private void handleInbound(Socket socket) {
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            while (!socket.isClosed()) {
                RaftProtocol.RaftMessage request = RaftProtocol.decode(in);
                RaftProtocol.RaftMessage response = dispatch(request);
                byte[] frame = RaftProtocol.encode(response.type(), response.requestId(), response.payload());
                synchronized (out) {
                    out.write(frame);
                    out.flush();
                }
            }
        } catch (Exception e) {
            if (running) {
                log.debug("[{}] 入站连接断开: {} ({})", nodeId, socket.getRemoteSocketAddress(), e.getMessage());
            }
        }
    }

    /**
     * 根据消息类型分发到本地 RaftNode 处理, 并组装响应消息
     */
    private RaftProtocol.RaftMessage dispatch(RaftProtocol.RaftMessage request) throws Exception {
        RaftNode node = localNode;
        if (node == null) {
            throw new IllegalStateException("RaftNode 尚未 bind, 无法处理入站请求");
        }

        return switch (request.type()) {
            case RaftProtocol.TYPE_VOTE_REQ -> {
                RequestVoteRequest voteRequest = RaftProtocol.payloadAs(request, RequestVoteRequest.class);
                log.debug("[{}] <- [{}] RequestVote(term={})", nodeId, voteRequest.candidateId(), voteRequest.term());
                RequestVoteResponse voteResponse = node.handleRequestVote(voteRequest);
                yield new RaftProtocol.RaftMessage(RaftProtocol.TYPE_VOTE_RESP, request.requestId(), voteResponse);
            }
            case RaftProtocol.TYPE_APPEND_REQ -> {
                AppendEntriesRequest appendRequest = RaftProtocol.payloadAs(request, AppendEntriesRequest.class);
                boolean isHeartbeat = appendRequest.entries().isEmpty();
                if (!isHeartbeat) {
                    log.debug("[{}] <- [{}] AppendEntries(term={}, entries={})",
                            nodeId, appendRequest.leaderId(), appendRequest.term(), appendRequest.entries().size());
                }
                AppendEntriesResponse appendResponse = node.handleAppendEntries(appendRequest);
                yield new RaftProtocol.RaftMessage(RaftProtocol.TYPE_APPEND_RESP, request.requestId(), appendResponse);
            }
            default -> throw new IllegalArgumentException("未知消息类型: " + request.type());
        };
    }

    // ================================================================
    //  出站: 向其他节点发送请求 (RaftRpcService 接口实现)
    // ================================================================

    @Override
    public RequestVoteResponse requestVote(NodeInfo target, RequestVoteRequest request) throws Exception {
        log.debug("[{}] -> [{}] RequestVote(term={})", nodeId, target.id(), request.term());
        RaftProtocol.RaftMessage response = sendAndWait(target,
                new RaftProtocol.RaftMessage(RaftProtocol.TYPE_VOTE_REQ, newRequestId(), request),
                RaftProtocol.TYPE_VOTE_RESP);
        return RaftProtocol.payloadAs(response, RequestVoteResponse.class);
    }

    @Override
    public AppendEntriesResponse appendEntries(NodeInfo target, AppendEntriesRequest request) throws Exception {
        boolean isHeartbeat = request.entries().isEmpty();
        if (!isHeartbeat) {
            log.debug("[{}] -> [{}] AppendEntries(term={}, entries={})",
                    nodeId, target.id(), request.term(), request.entries().size());
        }
        RaftProtocol.RaftMessage response = sendAndWait(target,
                new RaftProtocol.RaftMessage(RaftProtocol.TYPE_APPEND_REQ, newRequestId(), request),
                RaftProtocol.TYPE_APPEND_RESP);
        return RaftProtocol.payloadAs(response, AppendEntriesResponse.class);
    }

    /**
     * 发送请求并阻塞等待响应 (带超时)
     */
    private RaftProtocol.RaftMessage sendAndWait(NodeInfo target, RaftProtocol.RaftMessage request,
                                                 int expectedResponseType) throws Exception {
        String address = target.address();
        Socket socket = getOrCreateConnection(target);

        CompletableFuture<RaftProtocol.RaftMessage> future = new CompletableFuture<>();
        pendingResponses.put(request.requestId(), new PendingCall(address, future));

        try {
            // 编码并发送 (同一连接可能被多个虚拟线程并发写, 需要加锁)
            byte[] frame = RaftProtocol.encode(request.type(), request.requestId(), request.payload());
            OutputStream out = socket.getOutputStream();
            synchronized (out) {
                out.write(frame);
                out.flush();
            }

            // 等待读线程匹配到响应 (超时保护: 目标节点故障时不能永远挂起)
            RaftProtocol.RaftMessage response = future.get(rpcTimeoutMs, TimeUnit.MILLISECONDS);
            if (response.type() != expectedResponseType) {
                throw new IOException("响应类型不匹配: expected=" + expectedResponseType + ", actual=" + response.type());
            }
            return response;
        } catch (TimeoutException e) {
            pendingResponses.remove(request.requestId());
            removeConnection(address, socket);
            throw new IOException("RPC 超时 (" + rpcTimeoutMs + "ms) -> " + target.id(), e);
        } catch (ExecutionException e) {
            pendingResponses.remove(request.requestId());
            throw new IOException("RPC 失败 -> " + target.id() + ": " + e.getCause().getMessage(), e.getCause());
        } catch (Exception e) {
            // 写失败 (连接已断开) 等: 清理连接缓存, 下次发送自动重建
            pendingResponses.remove(request.requestId());
            removeConnection(address, socket);
            throw e;
        }
    }

    /**
     * 获取或创建到目标节点的连接 (每个目标一个长连接)
     */
    private Socket getOrCreateConnection(NodeInfo target) throws IOException {
        String address = target.address();
        Socket cached = connections.get(address);
        if (cached != null && !cached.isClosed()) {
            return cached;
        }

        synchronized (connections) {
            // 双重检查, 避免并发创建重复连接
            cached = connections.get(address);
            if (cached != null && !cached.isClosed()) {
                return cached;
            }
            Socket socket = new Socket(target.host(), target.port());
            socket.setTcpNoDelay(true);
            connections.put(address, socket);
            startResponseReader(target, socket);
            log.info("[{}] 建立到 {} 的连接", nodeId, address);
            return socket;
        }
    }

    /**
     * 启动响应读取线程: 持续读取该连接上的响应, 按 requestId 唤醒等待中的请求
     */
    private void startResponseReader(NodeInfo target, Socket socket) {
        Thread.ofVirtual().name("raft-rpc-reader-" + nodeId + "-" + target.id()).start(() -> {
            String address = target.address();
            try {
                InputStream in = socket.getInputStream();
                while (!socket.isClosed()) {
                    RaftProtocol.RaftMessage response = RaftProtocol.decode(in);
                    PendingCall call = pendingResponses.remove(response.requestId());
                    if (call != null) {
                        call.future().complete(response);
                    } else {
                        log.debug("[{}] 收到未知请求 ID 的响应: {}", nodeId, response.requestId());
                    }
                }
            } catch (Exception e) {
                if (!socket.isClosed()) {
                    log.info("[{}] 与 {} 的连接断开: {}", nodeId, address, e.getMessage());
                }
            } finally {
                // 连接断开: 快速失败该连接上所有等待中的请求 (RaftNode 会捕获并容忍)
                pendingResponses.entrySet().removeIf(entry -> {
                    if (entry.getValue().address().equals(address)) {
                        entry.getValue().future().completeExceptionally(
                                new IOException("连接已断开: " + address));
                        return true;
                    }
                    return false;
                });
                removeConnection(address, socket);
            }
        });
    }

    /**
     * 从连接缓存中移除并关闭连接
     */
    private void removeConnection(String address, Socket socket) {
        connections.remove(address, socket);
        try {
            socket.close();
        } catch (IOException ignored) {
        }
    }

    private String newRequestId() {
        return nodeId + "-" + UUID.randomUUID();
    }

    /**
     * 停止服务: 关闭监听端口与所有出站连接
     */
    public void stop() {
        running = false;
        try {
            if (serverSocket != null) {
                serverSocket.close();
            }
        } catch (IOException ignored) {
        }
        connections.values().forEach(socket -> {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        });
        connections.clear();
        pendingResponses.values().forEach(call ->
                call.future().completeExceptionally(new IOException("RPC 服务已停止")));
        pendingResponses.clear();
        log.info("[{}] Raft RPC 服务已停止", nodeId);
    }

    /**
     * 等待中的出站调用: 记录目标地址, 便于连接断开时精准失败
     */
    private record PendingCall(String address, CompletableFuture<RaftProtocol.RaftMessage> future) {
    }
}
