package com.study.distributed.manager.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.List;

/** 路径相对于 manager 的工作目录，通常从项目根目录启动。 */
@ConfigurationProperties(prefix = "manager")
public record ManagerProperties(String nodeJar, String logDirectory, String stateFile,
                                boolean autoStart, Cluster cluster) {
    public record Cluster(List<NodeDefinition> nodes) {}
    public record NodeDefinition(String id, String host, int rpcPort, int httpPort) {
        public NodeDefinition {
            if (id == null || !id.matches("node-[1-9][0-9]*")) {
                throw new IllegalArgumentException("节点 ID 必须为 node-N，N 为正整数");
            }
            // 本实现通过本地 ProcessBuilder 编排，拒绝远程地址与任意 URL，避免 SSRF。
            if (!"127.0.0.1".equals(host) && !"localhost".equals(host)) {
                throw new IllegalArgumentException("manager 仅支持本机节点，host 必须为 127.0.0.1 或 localhost");
            }
            if (rpcPort < 1024 || rpcPort > 65535 || httpPort < 1024 || httpPort > 65535 || rpcPort == httpPort) {
                throw new IllegalArgumentException("HTTP/RPC 端口必须在 1024..65535 且不同");
            }
        }
        public String peerSpec() { return id + "@" + host + ":" + rpcPort + ":" + httpPort; }
    }
}
