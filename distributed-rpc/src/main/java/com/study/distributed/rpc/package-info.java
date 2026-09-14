/**
 * <h2>distributed-rpc - 手写 RPC 框架</h2>
 *
 * <p>本模块从零实现一个 RPC (Remote Procedure Call) 框架，帮助理解:</p>
 * <ul>
 *   <li><b>协议设计</b>: 为什么需要自定义协议？TCP 粘包/拆包如何解决？</li>
 *   <li><b>序列化</b>: 不同序列化策略的优劣对比 (JSON vs 二进制)</li>
 *   <li><b>传输层</b>: Socket + Virtual Threads 的编程模型</li>
 *   <li><b>动态代理</b>: Dubbo/gRPC 等框架的核心原理</li>
 *   <li><b>服务注册</b>: 服务发现的基础概念</li>
 * </ul>
 *
 * <h3>阅读顺序</h3>
 * <ol>
 *   <li>{@link com.study.distributed.rpc.protocol.RpcRequest} - 请求模型</li>
 *   <li>{@link com.study.distributed.rpc.protocol.RpcProtocol} - 协议格式 (重点!)</li>
 *   <li>{@link com.study.distributed.rpc.transport.RpcServer} - 服务端处理流程</li>
 *   <li>{@link com.study.distributed.rpc.transport.RpcClient} - 客户端处理流程</li>
 *   <li>{@link com.study.distributed.rpc.proxy.RpcProxyFactory} - 动态代理</li>
 * </ol>
 *
 * <h3>思考题</h3>
 * <ul>
 *   <li>如果两个请求同时发送，响应如何正确匹配？(提示: requestId)</li>
 *   <li>如果服务端处理很慢，客户端会怎样？(提示: 超时机制)</li>
 *   <li>如何实现连接断开后的自动重连？</li>
 * </ul>
 */
package com.study.distributed.rpc;
