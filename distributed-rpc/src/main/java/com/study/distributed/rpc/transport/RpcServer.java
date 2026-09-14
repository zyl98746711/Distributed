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
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RPC 服务端 - 基于 Socket + Virtual Threads
 *
 * 学习要点:
 * 1. 使用 Virtual Threads 处理每个连接，替代传统线程池
 * 2. 服务注册: 将服务实例的方法映射到内存中
 * 3. 请求分发: 根据 serviceName + methodName 找到对应方法并反射调用
 */
public class RpcServer {

    private static final Logger log = LoggerFactory.getLogger(RpcServer.class);

    private final int port;
    private final Serializer serializer = new JsonSerializer();
    /** 服务名 -> 服务实例 */
    private final Map<String, Object> services = new ConcurrentHashMap<>();
    private volatile boolean running = false;
    private ServerSocket serverSocket;

    public RpcServer(int port) {
        this.port = port;
    }

    /**
     * 注册服务实例
     */
    public void registerService(String serviceName, Object serviceImpl) {
        services.put(serviceName, serviceImpl);
        log.info("注册服务: {}", serviceName);
    }

    /**
     * 启动服务器
     */
    public void start() throws IOException {
        serverSocket = new ServerSocket(port);
        running = true;
        log.info("RPC Server 启动在端口 {}", port);

        // 主线程: 接受连接，每个连接交给 Virtual Thread 处理
        Thread.ofVirtual().name("rpc-acceptor").start(() -> {
            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    // 每个连接一个 Virtual Thread
                    Thread.ofVirtual().name("rpc-handler-" + socket.getRemoteSocketAddress())
                            .start(() -> handleConnection(socket));
                } catch (IOException e) {
                    if (running) {
                        log.error("接受连接异常: {}", e.getMessage());
                    }
                }
            }
        });
    }

    /**
     * 处理一个客户端连接
     */
    private void handleConnection(Socket socket) {
        log.debug("处理连接: {}", socket.getRemoteSocketAddress());
        try (socket) {
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();

            while (!socket.isClosed()) {
                // 解码请求
                RpcProtocol.Frame frame = RpcProtocol.decode(in);
                RpcRequest request = serializer.deserialize(frame.data(), RpcRequest.class);

                // 处理请求
                RpcResponse response = handleRequest(request);

                // 编码并发送响应
                byte[] responseFrame = RpcProtocol.encodeResponse(response, serializer);
                synchronized (out) {
                    out.write(responseFrame);
                    out.flush();
                }
            }
        } catch (Exception e) {
            if (!socket.isClosed()) {
                log.debug("连接断开: {}", socket.getRemoteSocketAddress());
            }
        }
    }

    /**
     * 处理单个 RPC 请求 - 反射调用目标方法
     */
    private RpcResponse handleRequest(RpcRequest request) {
        Object service = services.get(request.serviceName());
        if (service == null) {
            return RpcResponse.fail(request.requestId(), "服务不存在: " + request.serviceName());
        }

        try {
            // 根据方法名和参数类型找到方法
            Method method = findMethod(service.getClass(), request.methodName(), request.paramTypes());
            method.setAccessible(true);
            Object result = method.invoke(service, request.params());
            return RpcResponse.ok(request.requestId(), result);
        } catch (InvocationTargetException e) {
            return RpcResponse.fail(request.requestId(), e.getCause().getMessage());
        } catch (Exception e) {
            return RpcResponse.fail(request.requestId(), "调用异常: " + e.getMessage());
        }
    }

    private Method findMethod(Class<?> clazz, String methodName, String[] paramTypes) throws Exception {
        if (paramTypes == null || paramTypes.length == 0) {
            return clazz.getMethod(methodName);
        }
        Class<?>[] paramClasses = new Class<?>[paramTypes.length];
        for (int i = 0; i < paramTypes.length; i++) {
            paramClasses[i] = resolveClass(paramTypes[i]);
        }
        return clazz.getMethod(methodName, paramClasses);
    }

    private Class<?> resolveClass(String className) throws ClassNotFoundException {
        return switch (className) {
            case "int" -> int.class;
            case "long" -> long.class;
            case "boolean" -> boolean.class;
            case "String", "java.lang.String" -> String.class;
            default -> Class.forName(className);
        };
    }

    public void stop() {
        running = false;
        try {
            if (serverSocket != null) serverSocket.close();
        } catch (IOException ignored) {}
    }
}
