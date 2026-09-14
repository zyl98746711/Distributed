package com.study.distributed.kv.controller;

import com.study.distributed.common.model.Result;
import com.study.distributed.kv.DistributedKVService;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

/**
 * KV 存储 HTTP 接口
 *
 * 提供 REST API 来操作分布式 KV 存储
 */
@RestController
@RequestMapping("/kv")
public class KVController {

    private final DistributedKVService kvService;

    public KVController(DistributedKVService kvService) {
        this.kvService = kvService;
    }

    @GetMapping("/{key}")
    public Result<String> get(@PathVariable String key) {
        String value = kvService.get(key);
        return value != null ? Result.ok(value) : Result.fail(404, "Key not found");
    }

    @PutMapping("/{key}")
    public Result<String> put(@PathVariable String key, @RequestParam String value) {
        try {
            kvService.put(key, value).join();
            return Result.ok("OK");
        } catch (Exception e) {
            return Result.fail(e.getMessage());
        }
    }

    @DeleteMapping("/{key}")
    public Result<String> delete(@PathVariable String key) {
        try {
            kvService.delete(key).join();
            return Result.ok("OK");
        } catch (Exception e) {
            return Result.fail(e.getMessage());
        }
    }

    @GetMapping("/all")
    public Result<Map<String, String>> getAll() {
        return Result.ok(kvService.getStateMachine().getKvStore().getAll());
    }

    /**
     * 查看节点状态
     */
    @GetMapping("/status")
    public Result<Map<String, Object>> status() {
        var raft = kvService.getRaftNode();
        return Result.ok(Map.of(
                "nodeId", raft.getNodeId(),
                "role", raft.getRole().name(),
                "term", raft.getCurrentTerm(),
                "leaderId", raft.getLeaderId() != null ? raft.getLeaderId() : "none",
                "commitIndex", raft.getCommitIndex(),
                "lastApplied", raft.getLastApplied(),
                "logSize", raft.getLogStore().size()
        ));
    }
}
