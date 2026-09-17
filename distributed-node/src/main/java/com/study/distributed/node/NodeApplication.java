package com.study.distributed.node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 独立 Raft 节点进程
 *
 * 每个 JVM 进程 = 一个完整的 Raft 节点:
 *   RaftNode + KVStateMachine + SocketRaftRpcService (真实 TCP 通信)
 *
 * 节点间通过 Socket 通信, 可以随时 kill / 重启单个节点, 观察:
 * - Leader 故障后重新选举
 * - Follower 宕机期间日志停滞, 恢复后追赶
 * - 进程隔离下真正的"分布式"
 *
 * 启动方式 (必须先 mvn clean package):
 * <pre>
 *   java -jar distributed-manager/target/distributed-manager-1.0.0-SNAPSHOT.jar
 *   manager 自动注入各节点的 ID、端口与 bootstrap 配置，无需 profile。
 * </pre>
 *
 * HTTP 接口 (管理端口见 application.yml 的 server.port):
 * - 节点状态: GET  /node/status
 * - 日志查看: GET  /node/log
 * - KV 写入:  PUT  /kv/{key}?value=xxx   (非 Leader 会自动转发到 Leader)
 * - KV 删除:  DELETE /kv/{key}
 * - KV 读取:  GET  /kv/{key}             (读本地状态机, 最终一致性)
 * - KV 全量:  GET  /kv/all
 */
@SpringBootApplication(scanBasePackages = "com.study.distributed.node")
public class NodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(NodeApplication.class, args);
    }
}

