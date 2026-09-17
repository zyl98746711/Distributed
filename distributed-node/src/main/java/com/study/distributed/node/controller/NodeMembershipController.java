package com.study.distributed.node.controller;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.common.model.Result;
import com.study.distributed.raft.core.RaftNode;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.*;

/** 成员变更必须写入 Raft 日志，不能直接修改本地 peers。 */
@RestController
@RequestMapping("/raft/members")
public class NodeMembershipController {
    private final RaftNode node;
    public NodeMembershipController(RaftNode node) { this.node = node; }

    @GetMapping
    public Result<Map<String, Object>> members() {
        synchronized (node) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("members", node.getMembership().members());
            view.put("oldMembers", node.getMembership().oldMembers());
            view.put("joint", node.getMembership().isJoint());
            view.put("changing", node.isMembershipChanging());
            view.put("committedMembers", node.getCommittedMembership().allNodes());
            view.put("leaderId", node.getLeaderId());
            return Result.ok(view);
        }
    }

    public record Change(String action, String id, String host, int rpcPort) {}

    @PostMapping
    public ResponseEntity<Result<?>> change(@RequestBody Change request) {
        if ((!"add".equals(request.action()) && !"remove".equals(request.action()))
                || request.id() == null || request.id().isBlank()) {
            return response(400, "action 必须为 add/remove，id 不能为空");
        }
        try {
            var result = node.changeMembership("add".equals(request.action()),
                    new NodeInfo(request.id(), request.host(), request.rpcPort())).get(5, TimeUnit.SECONDS);
            return ResponseEntity.ok(Result.ok(result));
        } catch (TimeoutException e) {
            return response(504, "成员变更等待超时，提交结果未知；请查询成员状态，勿直接回滚");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return response(503, "请求被中断，提交结果未知");
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RaftNode.NotLeaderException n) {
                Map<String, Object> data = new LinkedHashMap<>();
                data.put("leaderId", n.getLeaderId());
                return ResponseEntity.status(409).body(new Result<>(409, n.getMessage(), data));
            }
            return response(e.getCause() instanceof IllegalArgumentException ? 400 : 409, e.getCause().getMessage());
        } catch (IllegalArgumentException e) {
            return response(400, e.getMessage());
        }
    }

    private ResponseEntity<Result<?>> response(int code, String message) {
        return ResponseEntity.status(code).body(Result.fail(code, message));
    }
}
