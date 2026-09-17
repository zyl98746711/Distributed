package com.study.distributed.raft.core;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.raft.config.RaftConfig;
import com.study.distributed.raft.log.*;
import com.study.distributed.raft.membership.*;
import com.study.distributed.raft.rpc.*;
import org.junit.jupiter.api.*;
import org.springframework.test.util.ReflectionTestUtils;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import static org.junit.jupiter.api.Assertions.*;

/** 确定性触发选举，真实异步复制；不依赖随机选举计时。 */
class RaftMembershipTest {
    private final Map<String, RaftNode> network = new ConcurrentHashMap<>();
    private final List<RaftNode> created = new ArrayList<>();
    private final Set<String> unavailable = ConcurrentHashMap.newKeySet();
    private static final NodeInfo A = new NodeInfo("a", "localhost", 9001);
    private static final NodeInfo B = new NodeInfo("b", "localhost", 9002);
    private static final NodeInfo C = new NodeInfo("c", "localhost", 9003);
    private static final NodeInfo D = new NodeInfo("d", "localhost", 9004);
    private final RaftRpcService rpc = new RaftRpcService() {
        private RaftNode target(NodeInfo peer) {
            if (unavailable.contains(peer.id()) || !network.containsKey(peer.id())) throw new IllegalStateException("模拟断连");
            return network.get(peer.id());
        }
        public RequestVoteResponse requestVote(NodeInfo p, RequestVoteRequest r) { return target(p).handleRequestVote(r); }
        public AppendEntriesResponse appendEntries(NodeInfo p, AppendEntriesRequest r) { return target(p).handleAppendEntries(r); }
    };
    private RaftNode node(NodeInfo self, List<NodeInfo> members) {
        RaftNode node = new RaftNode(RaftConfig.of(self.id(), self.host(), self.port(), 100000, 110000, 30),
                members, bytes -> new String(bytes, java.nio.charset.StandardCharsets.UTF_8), rpc);
        network.put(self.id(), node);
        created.add(node);
        node.start();
        return node;
    }
    private void elect(RaftNode node) {
        ReflectionTestUtils.invokeMethod(node, "startElection");
        await(() -> node.getRole() == NodeRole.LEADER && node.getCommitIndex() > 0);
    }
    private void await(BooleanSupplier condition) {
        assertTimeoutPreemptively(Duration.ofSeconds(8), () -> {
            while (!condition.getAsBoolean()) Thread.sleep(20);
        });
    }
    @AfterEach void close() { created.forEach(RaftNode::stop); }

    @Test void jointRequiresBothMajoritiesAndRoundTrips() {
        Membership joint = Membership.joint(List.of(A, B, C), List.of(A, B, C, D));
        assertFalse(joint.hasQuorum(Set.of("a", "b")));
        assertTrue(joint.hasQuorum(Set.of("a", "b", "d")));
        assertEquals(joint, MemberCodec.decode(MemberCodec.encode(joint)));
        assertEquals(EntryType.COMMAND, new LogEntry(1, 1, null).type());
        assertThrows(IllegalArgumentException.class, () -> Membership.single(List.of(A, A)));
    }

    @Test void failedReplicationDoesNotCommitAndRecovers() throws Exception {
        List<NodeInfo> members = List.of(A, B, C);
        RaftNode leader = node(A, members);
        node(B, members); node(C, members);
        elect(leader);
        unavailable.addAll(Set.of("b", "c"));
        long before = leader.getCommitIndex();
        var write = leader.submitCommand("value".getBytes());
        Thread.sleep(150);
        assertFalse(write.isDone());
        assertEquals(before, leader.getCommitIndex());
        unavailable.remove("b");
        assertEquals("value", write.get(3, TimeUnit.SECONDS));
        assertTrue(leader.getCommitIndex() > before);
    }

    @Test void addRemoveAndRestartCatchUp() throws Exception {
        List<NodeInfo> original = List.of(A, B, C);
        RaftNode leader = node(A, original);
        node(B, original); node(C, original);
        elect(leader);
        leader.submitCommand("before".getBytes()).get(3, TimeUnit.SECONDS);
        RaftNode added = node(D, original);
        ReflectionTestUtils.invokeMethod(added, "startElection");
        assertEquals(NodeRole.FOLLOWER, added.getRole());
        assertEquals(4, leader.changeMembership(true, D).get(5, TimeUnit.SECONDS).members().size());
        await(() -> added.getLastApplied() == leader.getLastApplied());
        assertEquals(4, added.getCommittedMembership().members().size());
        added.stop(); network.remove(D.id());
        RaftNode restarted = node(D, List.of(A, B, C, D));
        await(() -> restarted.getLastApplied() == leader.getLastApplied());
        assertEquals(3, leader.changeMembership(false, D).get(5, TimeUnit.SECONDS).members().size());
        restarted.stop();
        leader.submitCommand("after".getBytes()).get(3, TimeUnit.SECONDS);
        assertFalse(leader.getMembership().contains("d"));
    }

    @Test void singleNodeExpansionNeedsNewNodeAndRejectsOverlappingChanges() throws Exception {
        RaftNode leader = node(A, List.of(A));
        elect(leader);
        var pending = leader.changeMembership(true, B);
        assertFalse(pending.isDone());
        assertTrue(leader.changeMembership(true, C).isCompletedExceptionally());
        node(B, List.of(A));
        assertEquals(2, pending.get(5, TimeUnit.SECONDS).members().size());
        leader.changeMembership(false, B).get(5, TimeUnit.SECONDS);
        assertTrue(leader.changeMembership(false, A).isCompletedExceptionally());
    }

    @Test void removedLeaderRetiresAndNewLeaderContinues() throws Exception {
        List<NodeInfo> members = List.of(A, B, C);
        RaftNode a = node(A, members), b = node(B, members), c = node(C, members);
        elect(a);
        a.changeMembership(false, A).get(5, TimeUnit.SECONDS);
        await(() -> a.getRole() == NodeRole.FOLLOWER && !b.isMembershipChanging() && !c.isMembershipChanging());
        elect(b);
        b.submitCommand("continued".getBytes()).get(3, TimeUnit.SECONDS);
        ReflectionTestUtils.invokeMethod(a, "startElection");
        assertEquals(NodeRole.FOLLOWER, a.getRole());
    }

    @org.junit.jupiter.params.ParameterizedTest
    @org.junit.jupiter.params.provider.ValueSource(booleans = {false, true})
    void newLeaderResumesJointOrUncommittedFinal(boolean finalAlreadyAppended) throws Exception {
        List<NodeInfo> original = List.of(A, B, C);
        RaftNode b = node(B, original), c = node(C, original), d = node(D, original);
        Membership joint = Membership.joint(original, List.of(A, B, C, D));
        List<LogEntry> entries = new ArrayList<>(List.of(new LogEntry(1, 1, null),
                new LogEntry(1, 2, EntryType.CONFIG_JOINT, MemberCodec.encode(joint))));
        if (finalAlreadyAppended) entries.add(new LogEntry(1, 3, EntryType.CONFIG_FINAL,
                MemberCodec.encode(Membership.single(joint.members()))));
        for (RaftNode follower : List.of(b, c, d)) {
            follower.handleAppendEntries(new AppendEntriesRequest(1, "a", 0, 0, entries,
                    finalAlreadyAppended ? 2 : 1));
        }
        elect(b);
        await(() -> !b.isMembershipChanging() && b.getCommittedMembership().members().size() == 4);
        assertEquals(1, b.getLogStore().getRange(1, b.getLogStore().lastIndex()).stream()
                .filter(e -> e.type() == EntryType.CONFIG_FINAL).count());
        b.submitCommand("recovered".getBytes()).get(3, TimeUnit.SECONDS);
    }

    @Test void shrinkingFinalCanCommitWithOnlyNewMajorityAfterLeaderFailure() throws Exception {
        List<NodeInfo> old = List.of(A, B, C, D);
        RaftNode b = node(B, old), c = node(C, old);
        List<LogEntry> entries = List.of(new LogEntry(1, 1, null),
                new LogEntry(1, 2, EntryType.CONFIG_JOINT, MemberCodec.encode(Membership.joint(old, List.of(A, B, C)))),
                new LogEntry(1, 3, EntryType.CONFIG_FINAL, MemberCodec.encode(Membership.single(List.of(A, B, C)))));
        for (RaftNode follower : List.of(b, c)) {
            follower.handleAppendEntries(new AppendEntriesRequest(1, "a", 0, 0, entries, 2));
        }
        // D 已移除，A 故障；B/C 是新配置多数派，但不是旧配置多数派。
        elect(b);
        b.submitCommand("available".getBytes()).get(3, TimeUnit.SECONDS);
        assertEquals(3, b.getCommittedMembership().members().size());
    }

    @Test void conflictingConfigurationIsReplacedAndUncommittedSuffixNotApplied() {
        RaftNode follower = node(B, List.of(A, B, C));
        Membership joint = Membership.joint(List.of(A, B, C), List.of(A, B, C, D));
        var config = new LogEntry(1, 1, EntryType.CONFIG_JOINT, MemberCodec.encode(joint));
        follower.handleAppendEntries(new AppendEntriesRequest(1, "a", 0, 0, List.of(config), 0));
        assertTrue(follower.getMembership().isJoint());
        follower.handleAppendEntries(new AppendEntriesRequest(2, "a", 0, 0,
                List.of(new LogEntry(2, 1, null), new LogEntry(2, 2, "x".getBytes())), 1));
        assertFalse(follower.getMembership().isJoint());
        assertEquals(2, follower.getLogStore().get(1).term());
        follower.handleAppendEntries(new AppendEntriesRequest(2, "a", 1, 2, List.of(), 2));
        assertEquals(1, follower.getCommitIndex());
    }
}
