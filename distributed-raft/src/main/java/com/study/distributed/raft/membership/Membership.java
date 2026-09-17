package com.study.distributed.raft.membership;

import com.study.distributed.common.model.NodeInfo;
import java.util.*;

/** 不可变成员配置；联合阶段必须分别满足旧、新成员的多数派。 */
public record Membership(List<NodeInfo> oldMembers, List<NodeInfo> members) {
    public Membership {
        oldMembers = oldMembers == null ? List.of() : List.copyOf(oldMembers);
        members = List.copyOf(members);
        validate(members);
        if (!oldMembers.isEmpty()) validate(oldMembers);
        for (NodeInfo old : oldMembers) {
            members.stream().filter(n -> n.id().equals(old.id())).forEach(n -> {
                if (!old.equals(n)) throw new IllegalArgumentException("同一节点 ID 不得改变地址");
            });
        }
    }

    private static void validate(List<NodeInfo> nodes) {
        if (nodes.isEmpty()) throw new IllegalArgumentException("成员集不能为空");
        Set<String> ids = new HashSet<>();
        Set<String> addresses = new HashSet<>();
        for (NodeInfo n : nodes) {
            if (n.id() == null || n.id().isBlank() || n.host() == null || n.host().isBlank()
                    || n.port() < 1 || n.port() > 65535 || !ids.add(n.id()) || !addresses.add(n.address())) {
                throw new IllegalArgumentException("成员 ID/地址无效或重复: " + n);
            }
        }
    }

    public static Membership single(List<NodeInfo> members) { return new Membership(List.of(), members); }
    public static Membership joint(List<NodeInfo> old, List<NodeInfo> members) { return new Membership(old, members); }
    public boolean isJoint() { return !oldMembers.isEmpty(); }
    public boolean contains(String id) { return allNodes().stream().anyMatch(n -> n.id().equals(id)); }
    public List<NodeInfo> allNodes() {
        Map<String, NodeInfo> union = new LinkedHashMap<>();
        oldMembers.forEach(n -> union.put(n.id(), n));
        members.forEach(n -> union.put(n.id(), n));
        return List.copyOf(union.values());
    }
    public boolean hasQuorum(Set<String> acknowledgements) {
        return majority(members, acknowledgements) && (!isJoint() || majority(oldMembers, acknowledgements));
    }
    private static boolean majority(List<NodeInfo> nodes, Set<String> acknowledgements) {
        return nodes.stream().filter(n -> acknowledgements.contains(n.id())).count() > nodes.size() / 2;
    }
}
