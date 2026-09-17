package com.study.distributed.manager.cluster;

import com.study.distributed.manager.config.ManagerProperties.NodeDefinition;

/** 部署记录与共识成员分离；PENDING 表示上一次变更结果需与 Leader 对账。 */
public final class ManagedNode {
    public enum State { STARTING, RUNNING, STOPPED }
    public enum MembershipState { MEMBER, PENDING, REMOVED }
    private final NodeDefinition definition;
    private final String processToken;
    private volatile MembershipState membership;
    private volatile State state = State.STOPPED;
    private volatile ProcessHandle process;
    public ManagedNode(NodeDefinition definition, MembershipState membership) {
        this(definition, membership, java.util.UUID.randomUUID().toString());
    }
    public ManagedNode(NodeDefinition definition, MembershipState membership, String processToken) {
        this.definition = definition;
        this.membership = membership;
        this.processToken = java.util.Objects.requireNonNull(processToken);
    }
    public String processToken() { return processToken; }
    public NodeDefinition definition() { return definition; }
    public MembershipState membership() { return membership; }
    public void membership(MembershipState membership) { this.membership = membership; }
    public State state() { return process != null && !process.isAlive() ? State.STOPPED : state; }
    public void state(State state) { this.state = state; }
    public ProcessHandle process() { return process; }
    public void process(ProcessHandle process) { this.process = process; }
}
