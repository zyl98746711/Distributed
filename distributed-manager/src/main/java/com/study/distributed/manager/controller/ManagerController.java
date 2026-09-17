package com.study.distributed.manager.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.study.distributed.common.model.Result;
import com.study.distributed.manager.client.NodeHttpClient;
import com.study.distributed.manager.cluster.*;
import com.study.distributed.manager.config.ManagerProperties.NodeDefinition;
import org.springframework.http.*;
import org.springframework.web.bind.annotation.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import static com.study.distributed.manager.cluster.ManagedNode.MembershipState.*;

/** 本地学习用控制平面；生产部署必须另加鉴权和高可用。 */
@RestController
@RequestMapping("/manager")
public class ManagerController {
    private final NodeProcessManager processes;
    private final LeaderLocator leaders;
    private final NodeHttpClient http;
    private final ReentrantLock operations = new ReentrantLock();
    public ManagerController(NodeProcessManager processes, LeaderLocator leaders, NodeHttpClient http) {
        this.processes = processes;
        this.leaders = leaders;
        this.http = http;
    }
    @GetMapping("/cluster")
    public Result<Map<String, Object>> cluster() { return Result.ok(leaders.cluster()); }

    public record AddNode(String host, Integer httpPort, Integer rpcPort) {}

    @PostMapping("/nodes")
    public ResponseEntity<Result<?>> add(@RequestBody(required = false) AddNode request) throws Exception {
        lock();
        ManagedNode node = null;
        boolean proposed = false;
        try {
            reconcile();
            node = processes.reserve(request == null ? null : request.host(),
                    request == null ? null : request.rpcPort(), request == null ? null : request.httpPort());
            // 新进程 bootstrap 不包含自己，因此在收到联合配置之前不会投票/参选。
            processes.start(node, true);
            proposed = true;
            Result<JsonNode> changed = http.change(leaders.leader(), "add", node.definition());
            if (changed.code() != 200) {
                return response(new Result<>(changed.code(), changed.message()
                        + "；保留部署记录及进程，可查询成员后重试删除/对账", node.definition()));
            }
            node.membership(MEMBER);
            processes.save();
            try {
                awaitCaughtUp(node);
            } catch (Exception e) {
                // 仅对明确成功的 ADD 做补偿；超时/断连不能当成未提交直接回滚。
                Result<JsonNode> rollback = http.change(leaders.leader(), "remove", node.definition());
                if (rollback.code() == 200) {
                    node.membership(REMOVED);
                    processes.save();
                    processes.stop(node);
                }
                return response(new Result<>(503, "新节点未追平，补偿结果: " + rollback.message(), node.definition()));
            }
            return response(Result.ok(node.definition()));
        } catch (Exception e) {
            if (node != null && !proposed) {
                processes.stop(node);
                node.membership(REMOVED);
                processes.save();
            }
            if (node != null && proposed) {
                return response(new Result<>(504, "成员变更结果未知，请查询集群后对账: " + e.getMessage(), node.definition()));
            }
            throw e;
        } finally { operations.unlock(); }
    }

    @DeleteMapping("/nodes/{id}")
    public ResponseEntity<Result<?>> remove(@PathVariable String id) throws Exception {
        lock();
        try {
            reconcile();
            ManagedNode node = processes.require(id);
            if (node.membership() == MEMBER) {
                Result<JsonNode> changed = http.change(leaders.leader(), "remove", node.definition());
                if (changed.code() != 200) return response(changed);
                node.membership(REMOVED);
                processes.save();
            }
            // 只有确认不在已提交配置中才停止，绝不先停进程再尝试缩容。
            processes.stop(node);
            leaders.invalidate();
            return response(Result.ok(node.definition()));
        } finally { operations.unlock(); }
    }

    @PostMapping("/nodes/{id}/start")
    public Result<NodeDefinition> start(@PathVariable String id) throws Exception {
        lock();
        try {
            ManagedNode node = processes.require(id);
            processes.start(node, true);
            return Result.ok(node.definition());
        } finally { operations.unlock(); }
    }

    @PostMapping("/nodes/{id}/stop")
    public Result<NodeDefinition> stop(@PathVariable String id) throws Exception {
        lock();
        try {
            ManagedNode node = processes.require(id);
            processes.stop(node);
            leaders.invalidate();
            return Result.ok(node.definition());
        } finally { operations.unlock(); }
    }

    private void lock() {
        if (!operations.tryLock()) throw new IllegalStateException("已有节点管理操作在执行，请稍后重试");
    }
    private void reconcile() throws Exception {
        JsonNode members = http.members(leaders.leader());
        if (members.path("changing").asBoolean()) throw new IllegalStateException("联合配置尚未提交完成");
        Set<String> ids = new HashSet<>();
        members.path("committedMembers").forEach(n -> ids.add(n.path("id").asText()));
        if (ids.isEmpty()) throw new IllegalStateException("无法确认已提交成员配置");
        for (String id : ids) processes.require(id);
        for (ManagedNode node : processes.snapshot()) {
            node.membership(ids.contains(node.definition().id()) ? MEMBER : REMOVED);
        }
        processes.save();
    }
    private void awaitCaughtUp(ManagedNode node) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        long target = http.status(leaders.leader()).path("commitIndex").asLong();
        while (System.nanoTime() < deadline) {
            JsonNode status = http.status(node.definition());
            if (status.path("lastApplied").asLong() >= target && !status.path("changing").asBoolean()) return;
            Thread.sleep(200);
        }
        throw new IllegalStateException("节点追赶超时");
    }

    @GetMapping("/kv/{key}")
    public ResponseEntity<Result<?>> get(@PathVariable String key) { return gateway(HttpMethod.GET, key, null); }
    @PutMapping("/kv/{key}")
    public ResponseEntity<Result<?>> put(@PathVariable String key, @RequestParam String value) {
        return gateway(HttpMethod.PUT, key, value);
    }
    @DeleteMapping("/kv/{key}")
    public ResponseEntity<Result<?>> delete(@PathVariable String key) { return gateway(HttpMethod.DELETE, key, null); }

    private ResponseEntity<Result<?>> gateway(HttpMethod method, String key, String value) {
        NodeDefinition leader = leaders.leader();
        try {
            Result<JsonNode> result = http.kv(leader, method, key, value);
            // 409 表示该节点明确未处理请求；仅此情况可以安全重新定位并重试一次。
            if (result.code() == 409) {
                leaders.invalidate();
                result = http.kv(leaders.leader(), method, key, value);
            }
            return response(result);
        } catch (Exception e) {
            leaders.invalidate();
            return response(Result.fail(502, "节点通信失败，写入结果可能未知，请查询确认后再重试"));
        }
    }
    private ResponseEntity<Result<?>> response(Result<?> result) {
        return ResponseEntity.status(result.code()).body(result);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<Result<?>> notFound(NoSuchElementException e) { return response(Result.fail(404, e.getMessage())); }
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Result<?>> badRequest(IllegalArgumentException e) { return response(Result.fail(400, e.getMessage())); }
    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Result<?>> conflict(IllegalStateException e) { return response(Result.fail(409, e.getMessage())); }
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Result<?>> failure(Exception e) {
        if (e instanceof InterruptedException) Thread.currentThread().interrupt();
        return response(Result.fail(503, "操作未完成: " + e.getMessage()));
    }
}
