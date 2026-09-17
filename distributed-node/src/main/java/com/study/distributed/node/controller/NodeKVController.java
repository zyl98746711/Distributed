package com.study.distributed.node.controller;

import com.study.distributed.common.model.Result;
import com.study.distributed.kv.DistributedKVService;
import com.study.distributed.node.config.PeerRegistry;
import com.study.distributed.node.config.PeerSpec;
import com.study.distributed.raft.core.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestClient;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 节点 KV 接口 - 每个节点进程自己的 HTTP 入口
 *
 * 与 demo 模式 (单 JVM 直接持有所有节点对象) 不同, 这里每个节点只能操作自己:
 * - 读: 直接读本地状态机 (最终一致性, 别的节点可能尚未复制到)
 * - 写: 自己是 Leader 则走 Raft 共识提交; 否则自动转发到 Leader 的 HTTP 端口
 *
 * "客户端可以连接任意节点, 写请求最终到达 Leader" 正是分布式系统的经典设计。
 */
@RestController
@RequestMapping("/kv")
public class NodeKVController {

    private static final Logger log = LoggerFactory.getLogger(NodeKVController.class);

    /** 转发标记 header: 防止 A -> B -> A 的循环转发 */
    private static final String FORWARD_HEADER = "X-Raft-Forwarded";

    /** 写入等待多数派确认的超时时间 (防止失去多数派时线程永久挂起) */
    private static final long WRITE_TIMEOUT_MS = 5000;

    private final DistributedKVService kvService;
    private final RaftNode raftNode;
    private final PeerRegistry registry;
    private final RestClient restClient = RestClient.create();

    public NodeKVController(DistributedKVService kvService, RaftNode raftNode, PeerRegistry registry) {
        this.kvService = kvService;
        this.raftNode = raftNode;
        this.registry = registry;
    }

    // ==================== 读操作 (本地状态机) ====================

    @GetMapping("/{key}")
    public Result<Object> get(@PathVariable String key) {
        String value = kvService.get(key);
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", raftNode.getNodeId());
        result.put("role", raftNode.getRole().name());
        result.put("key", key);
        result.put("value", value);
        return value != null ? Result.ok(result) : Result.fail(404, "Key not found (可能尚未复制到该节点)");
    }

    @GetMapping("/all")
    public Result<Map<String, String>> getAll() {
        return Result.ok(kvService.getStateMachine().getKvStore().getAll());
    }

    // ==================== 写操作 (自动转发到 Leader) ====================

    @PutMapping("/{key}")
    public Result<Object> put(@PathVariable String key, @RequestParam String value,
                              @RequestHeader(value = FORWARD_HEADER, required = false) String forwardedBy) {
        return executeWrite(key, forwardedBy,
                () -> kvService.put(key, value),
                leader -> forward(leader, () -> restClient.put()
                        .uri(builder -> builder.scheme("http")
                                .host(leader.host())
                                .port(leader.httpPort())
                                .path("/kv/{key}")
                                .queryParam("value", value)
                                .build(key))
                        .header(FORWARD_HEADER, raftNode.getNodeId())
                        .retrieve()
                        .body(new ParameterizedTypeReference<Result<Object>>() {
                        })));
    }

    @DeleteMapping("/{key}")
    public Result<Object> delete(@PathVariable String key,
                                 @RequestHeader(value = FORWARD_HEADER, required = false) String forwardedBy) {
        return executeWrite(key, forwardedBy,
                () -> kvService.delete(key),
                leader -> forward(leader, () -> restClient.delete()
                        .uri(builder -> builder.scheme("http")
                                .host(leader.host())
                                .port(leader.httpPort())
                                .path("/kv/{key}")
                                .build(key))
                        .header(FORWARD_HEADER, raftNode.getNodeId())
                        .retrieve()
                        .body(new ParameterizedTypeReference<Result<Object>>() {
                        })));
    }

    /**
     * 写操作通用流程:
     *   1. 本地提交 (只有 Leader 会成功, 其他节点立即返回 NotLeader)
     *   2. 失败原因是 NotLeader -> 转发到 Leader 的 HTTP 端口
     *   3. 已是转发来的请求 (防循环) 或 Leader 未知 -> 直接失败
     */
    private Result<Object> executeWrite(String key, String forwardedBy,
                                        Supplier<CompletableFuture<String>> localOp,
                                        Function<PeerSpec, Result<Object>> forwardOp) {
        try {
            localOp.get().get(WRITE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            Map<String, Object> ok = new LinkedHashMap<>();
            ok.put("nodeId", raftNode.getNodeId());
            ok.put("key", key);
            ok.put("status", "OK");
            ok.put("leaderId", raftNode.getLeaderId());
            return Result.ok(ok);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof RaftNode.NotLeaderException notLeader) {
                if (forwardedBy != null) {
                    // 该请求是别的节点转发来的, 自己仍不是 Leader (如刚发生选举), 拒绝二次转发
                    return Result.fail(409, "节点 " + raftNode.getNodeId()
                            + " 不是 Leader 且请求来自转发节点 " + forwardedBy + ", 拒绝二次转发");
                }
                PeerSpec leader = registry.byId(notLeader.getLeaderId());
                if (leader == null) {
                    return Result.fail(409, "Leader 未知或不在启动注册表，请经由 manager 网关 (/manager/kv) 访问");
                }
                log.info("[{}] 不是 Leader, 转发写请求到 {} ({}:{})",
                        raftNode.getNodeId(), leader.id(), leader.host(), leader.httpPort());
                return forwardOp.apply(leader);
            }
            return Result.fail("写入失败: " + cause.getMessage());
        } catch (TimeoutException e) {
            return Result.fail(504, "写入超时: " + (WRITE_TIMEOUT_MS / 1000)
                    + "s 内未获得多数派确认 (集群是否已失去多数派节点?)");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return Result.fail("写入被中断");
        } catch (Exception e) {
            return Result.fail("写入异常: " + e.getMessage());
        }
    }

    /**
     * 通过 HTTP 转发到 Leader 节点
     */
    private Result<Object> forward(PeerSpec leader, Supplier<Result<Object>> call) {
        try {
            return call.get();
        } catch (Exception e) {
            return Result.fail(502, "转发到 Leader (" + leader.id() + ") 失败: " + e.getMessage());
        }
    }
}
