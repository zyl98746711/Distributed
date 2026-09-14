package com.study.distributed.demo;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 分布式学习框架 - 演示应用 (3 节点 Raft 集群)
 *
 * 启动后可通过 HTTP API 体验:
 * - 集群状态: GET /cluster/status
 * - KV 读写:  PUT /kv/{nodeId}/{key}?value=xxx
 * - KV 读取: GET /kv/{nodeId}/{key}
 * - 自动路由: PUT /kv/leader/{key}?value=xxx
 * - 数据对比: GET /kv/all/data
 * - ID 生成: GET /demo/id
 * - 限流测试: GET /demo/ratelimit/compare
 */
@SpringBootApplication(scanBasePackages = "com.study.distributed.demo")
public class DemoApplication {

    public static void main(String[] args) {
        SpringApplication.run(DemoApplication.class, args);
    }
}
