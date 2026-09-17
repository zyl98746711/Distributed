package com.study.distributed.manager.cluster;

import com.fasterxml.jackson.databind.JsonNode;
import com.study.distributed.manager.client.NodeHttpClient;
import com.study.distributed.manager.config.ManagerProperties.NodeDefinition;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.*;

/** manager 只发现 Leader，不参与 Raft 投票。 */
@Component
public class LeaderLocator {
    private final NodeProcessManager processes;
    private final NodeHttpClient http;
    private volatile String leaderId;
    public LeaderLocator(NodeProcessManager processes, NodeHttpClient http) {
        this.processes = processes;
        this.http = http;
    }
    @Scheduled(fixedDelay = 2000)
    public synchronized void refresh() {
        String found = null;
        long term = -1;
        for (ManagedNode node : processes.snapshot()) {
            if (node.membership() == ManagedNode.MembershipState.REMOVED) continue;
            try {
                JsonNode status = http.status(node.definition());
                if ("LEADER".equals(status.path("role").asText()) && status.path("term").asLong() > term) {
                    found = node.definition().id();
                    term = status.path("term").asLong();
                }
            } catch (Exception ignored) { }
        }
        leaderId = found;
    }
    public NodeDefinition leader() {
        String cached = leaderId;
        if (cached != null) {
            try {
                var node = processes.require(cached);
                if (node.membership() != ManagedNode.MembershipState.REMOVED
                        && "LEADER".equals(http.status(node.definition()).path("role").asText())) return node.definition();
            } catch (Exception ignored) { }
        }
        refresh();
        String current = leaderId;
        if (current == null) throw new IllegalStateException("当前无可用 Leader，请等待选举或恢复多数派");
        return processes.require(current).definition();
    }
    public void invalidate() { leaderId = null; }
    public Map<String, Object> cluster() {
        refresh();
        List<Map<String, Object>> views = new ArrayList<>();
        for (ManagedNode node : processes.snapshot()) {
            Map<String, Object> view = new LinkedHashMap<>();
            view.put("definition", node.definition());
            view.put("membership", node.membership());
            view.put("state", node.state());
            try {
                view.put("status", http.status(node.definition()));
                view.put("state", ManagedNode.State.RUNNING);
            } catch (Exception e) { view.put("reachable", false); }
            views.add(view);
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("leaderId", leaderId);
        result.put("nodes", views);
        return result;
    }
}
