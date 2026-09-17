package com.study.distributed.node.controller;

import com.study.distributed.common.model.Result;
import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.raft.core.RaftNode;
import com.study.distributed.raft.log.LogEntry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 节点管理接口 - 观察节点内部状态
 *
 * 结合 kill / 重启 节点做故障实验:
 * - 对比各节点的 role / term / commitIndex, 观察选举过程
 * - 对比各节点的日志, 观察复制与追赶过程
 */
@RestController
@RequestMapping("/node")
public class NodeController {

    private final RaftNode raftNode;
    private final String processToken;
    public NodeController(RaftNode raftNode,
                          @org.springframework.beans.factory.annotation.Value("${raft.process-token:}") String processToken) {
        this.raftNode = raftNode;
        this.processToken = processToken;
    }

    /**
     * 节点状态: 角色 / 任期 / Leader / 提交进度 / 日志规模
     */
    @GetMapping("/status")
    public Result<Map<String, Object>> status() {
        Map<String, Object> status = new LinkedHashMap<>();
        status.put("nodeId", raftNode.getNodeId());
        status.put("role", raftNode.getRole().name());
        status.put("term", raftNode.getCurrentTerm());
        status.put("leaderId", raftNode.getLeaderId() != null ? raftNode.getLeaderId() : "none");
        status.put("commitIndex", raftNode.getCommitIndex());
        status.put("lastApplied", raftNode.getLastApplied());
        status.put("logSize", raftNode.getLogStore().size());
        status.put("peers", raftNode.getMembership().allNodes().stream().map(NodeInfo::id).toList());
        status.put("joint", raftNode.getMembership().isJoint());
        status.put("changing", raftNode.isMembershipChanging());
        status.put("pid", ProcessHandle.current().pid());
        status.put("processToken", processToken);
        status.put("processStartedAt", ProcessHandle.current().info().startInstant().map(Object::toString).orElse(""));
        status.put("rpcPort", raftNode.getConfig().rpcPort());
        return Result.ok(status);
    }

    /**
     * 日志条目: 观察日志复制进度 (对比各节点的 lastIndex / commitIndex)
     */
    @GetMapping("/log")
    public Result<Map<String, Object>> log() {
        long lastIndex = raftNode.getLogStore().lastIndex();
        List<LogEntry> entries = raftNode.getLogStore().getRange(1, lastIndex);

        List<Map<String, Object>> items = new ArrayList<>();
        for (LogEntry entry : entries) {
            Map<String, Object> item = new LinkedHashMap<>();
            item.put("index", entry.index());
            item.put("term", entry.term());
            item.put("type", entry.type());
            item.put("command", entry.command() != null
                    ? new String(entry.command(), StandardCharsets.UTF_8) : null);
            items.add(item);
        }

        Map<String, Object> result = new LinkedHashMap<>();
        result.put("nodeId", raftNode.getNodeId());
        result.put("role", raftNode.getRole().name());
        result.put("lastIndex", lastIndex);
        result.put("commitIndex", raftNode.getCommitIndex());
        result.put("lastApplied", raftNode.getLastApplied());
        result.put("entries", items);
        return Result.ok(result);
    }
}
