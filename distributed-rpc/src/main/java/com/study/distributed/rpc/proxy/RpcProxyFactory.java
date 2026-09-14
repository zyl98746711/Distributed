package com.study.distributed.rpc.proxy;

import com.study.distributed.common.exception.RpcException;
import com.study.distributed.rpc.protocol.RpcRequest;
import com.study.distributed.rpc.protocol.RpcResponse;
import com.study.distributed.rpc.transport.RpcClient;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * RPC 代理工厂 - 使用 JDK 动态代理生成客户端 Stub
 *
 * 学习要点:
 * 1. 动态代理让远程调用看起来像本地调用
 * 2. InvocationHandler 拦截方法调用，转换为 RPC 请求
 * 3. 这是 Dubbo/gRPC 等框架的核心原理之一
 *
 * 使用示例:
 * <pre>
 *   HelloService hello = RpcProxyFactory.create(HelloService.class, "localhost", 8080);
 *   String result = hello.sayHi("world"); // 实际走网络调用
 * </pre>
 */
public class RpcProxyFactory {

    /**
     * 创建远程服务的代理实例
     */
    @SuppressWarnings("unchecked")
    public static <T> T create(Class<T> serviceInterface, String host, int port, RpcClient client) {
        return (T) Proxy.newProxyInstance(
                serviceInterface.getClassLoader(),
                new Class<?>[]{serviceInterface},
                new RpcInvocationHandler(serviceInterface.getName(), host, port, client)
        );
    }

    /**
     * RPC 调用处理器
     */
    private static class RpcInvocationHandler implements InvocationHandler {

        private final String serviceName;
        private final String host;
        private final int port;
        private final RpcClient client;

        RpcInvocationHandler(String serviceName, String host, int port, RpcClient client) {
            this.serviceName = serviceName;
            this.host = host;
            this.port = port;
            this.client = client;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            // 跳过 Object 方法
            if (method.getDeclaringClass() == Object.class) {
                return method.invoke(this, args);
            }

            // 构建参数类型名数组
            String[] paramTypes = null;
            Class<?>[] paramClasses = method.getParameterTypes();
            if (paramClasses.length > 0) {
                paramTypes = new String[paramClasses.length];
                for (int i = 0; i < paramClasses.length; i++) {
                    paramTypes[i] = paramClasses[i].getName();
                }
            }

            // 构建 RPC 请求
            RpcRequest request = new RpcRequest(
                    RpcRequest.nextId(),
                    serviceName,
                    method.getName(),
                    paramTypes,
                    args
            );

            // 发送请求
            RpcResponse response = client.send(host, port, request);

            if (response.success()) {
                return response.result();
            } else {
                throw new RpcException("RPC 调用失败: " + response.error());
            }
        }
    }
}
