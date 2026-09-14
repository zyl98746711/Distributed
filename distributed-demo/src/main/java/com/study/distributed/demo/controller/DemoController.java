package com.study.distributed.demo.controller;

import com.study.distributed.common.model.Result;
import com.study.distributed.demo.config.DemoConfig.RaftCluster;
import com.study.distributed.id.api.IdGenerator;
import com.study.distributed.id.snowflake.SnowflakeIdGenerator;
import com.study.distributed.kv.DistributedKVService;
import com.study.distributed.ratelimit.algorithm.*;
import com.study.distributed.ratelimit.api.RateLimiter;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 多节点演示控制器
 *
 * API 设计:
 *   /cluster/status              - 查看集群整体状态
 *   /kv/{nodeId}/{key}           - 操作指定节点的 KV
 *   /kv/leader/{key}             - 自动路由到 Leader 节点
 *   /demo/id                     - 生成 ID
 *   /demo/ratelimit/compare      - 限流算法对比
 */
@RestController
@RequestMapping
public class DemoController {

    private final RaftCluster cluster;
    private final IdGenerator idGenerator;

    // 四种限流器实例用于对比
    private final RateLimiter fixedWindow = new FixedWindowLimiter(10, 1000);
    private final RateLimiter slidingWindow = new SlidingWindowLimiter(10, 1000);
    private final RateLimiter tokenBucket = new TokenBucketLimiter(10, 100);
    private final RateLimiter leakyBucket = new LeakyBucketLimiter(100);

    public DemoController(RaftCluster cluster, IdGenerator idGenerator) {
        this.cluster = cluster;
        this.idGenerator = idGenerator;
    }

    // ==================== 集群管理 ====================

    /**
     * 查看集群状态 - 展示所有节点的角色、任期、日志等信息
     */
    @GetMapping("/cluster/status")
    public Result<Map<String, Object>> clusterStatus() {
        return Result.ok(cluster.getClusterStatus());
    }

    // ==================== KV 操作 (指定节点) ====================

    /**
     * 读取指定节点的 KV
     * GET /kv/node-1/mykey
     */
    @GetMapping("/kv/{nodeId}/{key}")
    public Result<Object> getFromNode(@PathVariable String nodeId, @PathVariable String key) {
        DistributedKVService kv = cluster.getKvService(nodeId);
        if (kv == null) {
            return Result.fail(404, "节点不存在: " + nodeId);
        }
        String value = kv.get(key);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", nodeId);
        result.put("key", key);
        result.put("value", value);
        result.put("nodeRole", kv.getRaftNode().getRole().name());
        return value != null ? Result.ok(result) : Result.fail(404, "Key not found");
    }

    /**
     * 写入指定节点的 KV (必须是 Leader)
     * PUT /kv/node-1/mykey?value=hello
     */
    @PutMapping("/kv/{nodeId}/{key}")
    public Result<Object> putToNode(@PathVariable String nodeId, @PathVariable String key,
                                     @RequestParam String value) {
        DistributedKVService kv = cluster.getKvService(nodeId);
        if (kv == null) {
            return Result.fail(404, "节点不存在: " + nodeId);
        }
        try {
            kv.put(key, value).join();
            Map<String, Object> result = new LinkedHashMap<>();
            result.put("nodeId", nodeId);
            result.put("key", key);
            result.put("value", value);
            result.put("status", "OK");
            return Result.ok(result);
        } catch (Exception e) {
            // 如果不是 Leader，提示 Leader 是谁
            String leaderId = cluster.getLeaderId();
            Map<String, Object> err = new LinkedHashMap<>();
            err.put("nodeId", nodeId);
            err.put("error", e.getMessage());
            err.put("leaderId", leaderId);
            err.put("hint", "写操作需要发给 Leader 节点，请尝试 /kv/" + leaderId + "/" + key);
            return Result.fail(err.toString());
        }
    }

    /**
     * 删除指定节点的 KV
     * DELETE /kv/node-1/mykey
     */
    @DeleteMapping("/kv/{nodeId}/{key}")
    public Result<Object> deleteFromNode(@PathVariable String nodeId, @PathVariable String key) {
        DistributedKVService kv = cluster.getKvService(nodeId);
        if (kv == null) {
            return Result.fail(404, "节点不存在: " + nodeId);
        }
        try {
            kv.delete(key).join();
            return Result.ok(Map.of("nodeId", nodeId, "key", key, "status", "DELETED"));
        } catch (Exception e) {
            return Result.fail(e.getMessage());
        }
    }

    /**
     * 自动路由到 Leader 写入
     * PUT /kv/leader/mykey?value=hello
     */
    @PutMapping("/kv/leader/{key}")
    public Result<Object> putToLeader(@PathVariable String key, @RequestParam String value) {
        String leaderId = cluster.getLeaderId();
        if (leaderId == null) {
            return Result.fail("当前没有 Leader，集群正在选举中...");
        }
        DistributedKVService kv = cluster.getKvService(leaderId);
        try {
            kv.put(key, value).join();
            return Result.ok(Map.of(
                    "leaderId", leaderId,
                    "key", key,
                    "value", value,
                    "status", "OK"
            ));
        } catch (Exception e) {
            return Result.fail(e.getMessage());
        }
    }

    /**
     * 查看所有节点的 KV 数据 (对比一致性)
     * GET /kv/all/data
     */
    @GetMapping("/kv/all/data")
    public Result<Map<String, Object>> allNodesData() {
        Map<String, Object> result = new LinkedHashMap<>();
        for (var entry : cluster.getAllKvServices().entrySet()) {
            String nodeId = entry.getKey();
            DistributedKVService kv = entry.getValue();
            result.put(nodeId, Map.of(
                    "role", kv.getRaftNode().getRole().name(),
                    "data", kv.getStateMachine().getKvStore().getAll()
            ));
        }
        return Result.ok(result);
    }

    // ==================== ID 生成 ====================

    @GetMapping("/demo/id")
    public Result<Map<String, Object>> generateId() {
        long id = idGenerator.nextId();
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("id", id);
        result.put("timestamp", SnowflakeIdGenerator.getTimestamp(id));
        result.put("workerId", SnowflakeIdGenerator.getWorkerId(id));
        result.put("sequence", SnowflakeIdGenerator.getSequence(id));
        return Result.ok(result);
    }

    @GetMapping("/demo/id/batch")
    public Result<Map<String, Object>> batchGenerateIds() {
        int count = 10000;
        long start = System.nanoTime();
        List<Long> ids = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            ids.add(idGenerator.nextId());
        }
        long elapsed = System.nanoTime() - start;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("count", count);
        result.put("elapsedMs", elapsed / 1_000_000.0);
        result.put("perSecond", count / (elapsed / 1_000_000_000.0));
        result.put("sampleIds", ids.subList(0, 5));
        return Result.ok(result);
    }

    // ==================== 限流对比 ====================

    @GetMapping("/demo/ratelimit/compare")
    public Result<Map<String, Object>> rateLimitCompare() {
        int testRequests = 20;

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("fixedWindow", testLimiter(fixedWindow, testRequests));
        result.put("slidingWindow", testLimiter(slidingWindow, testRequests));
        result.put("tokenBucket", testLimiter(tokenBucket, testRequests));
        result.put("leakyBucket", testLimiter(leakyBucket, testRequests));
        result.put("description", "每种限流器测试 " + testRequests + " 次请求，对比通过/拒绝数量");
        return Result.ok(result);
    }

    private Map<String, Object> testLimiter(RateLimiter limiter, int requests) {
        int passed = 0;
        int rejected = 0;
        for (int i = 0; i < requests; i++) {
            if (limiter.tryAcquire()) {
                passed++;
            } else {
                rejected++;
            }
        }
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("passed", passed);
        r.put("rejected", rejected);
        r.put("status", limiter.getStatus());
        return r;
    }
}
