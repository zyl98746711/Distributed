package com.study.distributed.raft.core;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.raft.config.RaftConfig;
import com.study.distributed.raft.log.InMemoryLogStore;
import com.study.distributed.raft.log.LogEntry;
import com.study.distributed.raft.log.LogStore;
import com.study.distributed.raft.log.EntryType;
import com.study.distributed.raft.membership.MemberCodec;
import com.study.distributed.raft.membership.Membership;
import com.study.distributed.raft.rpc.*;
import com.study.distributed.raft.state.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Raft 节点核心实现
 *
 * 这是 Raft 共识算法的核心，实现了三大功能:
 * 1. Leader Election (领导者选举)
 * 2. Log Replication (日志复制)
 * 3. Safety (安全性保证)
 *
 * 学习要点:
 * - 每个节点维护: currentTerm, votedFor, log, commitIndex, lastApplied
 * - Leader 额外维护: nextIndex[], matchIndex[] (每个 Follower 一个)
 * - 选举超时随机化是避免活锁的关键
 */
public class RaftNode {

    private static final Logger log = LoggerFactory.getLogger(RaftNode.class);

    // ====== 持久化状态 (所有节点) ======
    private volatile long currentTerm = 0;
    private volatile String votedFor = null;
    private final LogStore logStore;

    // ====== 易失状态 (所有节点) ======
    private volatile NodeRole role = NodeRole.FOLLOWER;
    private volatile String leaderId = null;
    private volatile long commitIndex = 0;
    private volatile long lastApplied = 0;

    // ====== 易失状态 (Leader 独有, 每次选举后重置) ======
    /** 对每个 Follower 要发送的下一条日志索引 */
    private final Map<String, Long> nextIndex = new ConcurrentHashMap<>();
    /** 对每个 Follower 已知的最高匹配日志索引 */
    private final Map<String, Long> matchIndex = new ConcurrentHashMap<>();

    // ====== 配置与依赖 ======
    private final RaftConfig config;
    private final Membership bootstrapMembership;
    private volatile Membership activeMembership;
    private volatile Membership committedMembership;
    private long configIndex;
    private long jointIndex;
    private long finalIndex;
    private final Map<Long, CompletableFuture<Object>> pendingCommits = new HashMap<>();
    private CompletableFuture<Membership> pendingMembership;
    private final Set<String> replicating = ConcurrentHashMap.newKeySet();
    private final StateMachine stateMachine;
    private final RaftRpcService rpcService;

    // ====== 定时与选举 ======
    private volatile long lastHeartbeatTime = System.currentTimeMillis();
    private final Random random = new Random();
    private final AtomicBoolean running = new AtomicBoolean(true);

    // 选举投票计数
    private final Set<String> grantedVoters = new HashSet<>();

    public RaftNode(RaftConfig config, List<NodeInfo> peers, StateMachine stateMachine, RaftRpcService rpcService) {
        this.config = config;
        this.bootstrapMembership = Membership.single(peers);
        this.activeMembership = bootstrapMembership;
        this.committedMembership = bootstrapMembership;
        this.stateMachine = stateMachine;
        this.rpcService = rpcService;
        this.logStore = new InMemoryLogStore();
    }

    /**
     * 启动 Raft 节点
     */
    public void start() {
        log.info("[{}] Raft 节点启动, peers={}", config.nodeId(), activeMembership.allNodes().size());

        // 选举超时检查线程
        Thread.ofVirtual().name("raft-election-" + config.nodeId()).start(() -> {
            while (running.get()) {
                try {
                    Thread.sleep(100);
                    checkElectionTimeout();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        // Leader 心跳线程
        Thread.ofVirtual().name("raft-heartbeat-" + config.nodeId()).start(() -> {
            while (running.get()) {
                try {
                    Thread.sleep(config.heartbeatInterval());
                    if (role == NodeRole.LEADER) {
                        sendHeartbeats();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    // ================================================================
    //  1. Leader Election (领导者选举)
    // ================================================================

    /**
     * 检查选举超时 - Follower 在超时后变为 Candidate 发起选举
     *
     * 算法原理:
     *   Raft 使用时钟超时来检测 Leader 失效。当 Follower 在超时时间内
     *   没有收到任何 Leader 的心跳 (AppendEntries)，就认为 Leader 已失效，
     *   自己发起新一轮选举。
     *
     * 关键点:
     *   超时时间 = random(electionTimeoutMin, electionTimeoutMax)
     *   随机化是关键! 如果所有节点超时时间相同，会同时发起选举，
     *   导致票数分裂，无人当选。随机化让某个节点先超时，大概率赢得选举。
     */
    private synchronized void checkElectionTimeout() {
        if (!running.get() || !activeMembership.contains(config.nodeId())) return;
        if (role == NodeRole.LEADER) return; // Leader 不需要检查超时

        long elapsed = System.currentTimeMillis() - lastHeartbeatTime;
        long timeout = randomElectionTimeout();

        if (elapsed > timeout) {
            log.info("[{}] 选举超时 ({}ms > {}ms), 发起选举", config.nodeId(), elapsed, timeout);
            startElection();
        }
    }

    /**
     * 生成随机选举超时时间
     *
     * 为什么随机? 举例说明:
     *   3 个节点 A/B/C，超时都是 3s
     *   → 3s 后三个同时变为 Candidate → 各得 1 票 → 无人过半 → 等待下一轮
     *   → 可能反复出现 (活锁)
     *
     *   随机化后:
     *   → A 超时 2.1s, B 超时 3.5s, C 超时 4.2s
     *   → A 先发起选举，B/C 还是 Follower，会投票给 A
     *   → A 获得 3 票 (含自己)，当选 Leader
     */
    private long randomElectionTimeout() {
        return config.electionTimeoutMin() +
               random.nextInt(config.electionTimeoutMax() - config.electionTimeoutMin());
    }

    /**
     * 发起选举 - 变为 Candidate，增加任期，请求投票
     *
     * 选举流程 (Raft 论文 Section 5.2):
     *   1. 角色变为 Candidate
     *   2. currentTerm++ (开始新任期)
     *   3. 投票给自己 (votedFor = self)
     *   4. 向所有其他节点发送 RequestVote RPC
     *   5. 如果获得多数票 → 成为 Leader
     *   6. 如果发现更高任期 → 退回 Follower
     *   7. 如果超时未获得多数票 → 等待下一轮选举
     */
    private synchronized void startElection() {
        if (!running.get() || !activeMembership.contains(config.nodeId())) return;
        // 变为 Candidate
        role = NodeRole.CANDIDATE;
        currentTerm++;
        votedFor = config.nodeId();
        grantedVoters.clear();
        grantedVoters.add(config.nodeId());
        lastHeartbeatTime = System.currentTimeMillis();

        log.info("[{}] 成为 Candidate, term={}", config.nodeId(), currentTerm);

        // 检查是否已经获得多数票 (单节点集群)
        if (activeMembership.hasQuorum(grantedVoters)) {
            becomeLeader();
            return;
        }

        // 并行向所有 peer 发送 RequestVote
        RequestVoteRequest request = new RequestVoteRequest(currentTerm, config.nodeId(),
                logStore.lastIndex(), logStore.lastTerm());
        for (NodeInfo peer : activeMembership.allNodes()) {
            if (peer.id().equals(config.nodeId())) continue;
            Thread.ofVirtual().start(() -> {
                try {

                    RequestVoteResponse response = rpcService.requestVote(peer, request);

                    synchronized (this) {
                        if (response.term() > currentTerm) {
                            // 发现更高任期，退回 Follower
                            stepDown(response.term());
                            return;
                        }

                        if (running.get() && role == NodeRole.CANDIDATE && request.term() == currentTerm
                                && response.term() == currentTerm && response.voteGranted()) {
                            grantedVoters.add(peer.id());
                            if (activeMembership.hasQuorum(grantedVoters)) {
                                becomeLeader();
                            }
                        }
                    }
                } catch (Exception e) {
                    log.debug("[{}] RequestVote 失败 -> {}: {}", config.nodeId(), peer.id(), e.getMessage());
                }
            });
        }
    }

    /**
     * 处理 RequestVote RPC 请求 (作为 Follower 收到投票请求)
     *
     * 投票决策规则 (Raft 论文 Section 5.4):
     *   一个节点在一个任期内最多投一票，且遵循 "先到先得" 原则。
     *
     *   拒绝投票的条件 (满足任一):
     *   1. 候选人任期 < 当前任期 (过时的候选人)
     *   2. 本节点在本任期已投过票给别人
     *   3. 候选人的日志不够新 (安全性关键!)
     *
     *   日志完整性检查:
     *   如果候选人的最后一条日志的任期比自己小，或者任期相同但索引更小，
     *   说明候选人的日志不够完整，不能成为 Leader (否则已提交的数据可能丢失)。
     */
    public synchronized RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        if (!running.get() || !activeMembership.contains(config.nodeId())
                || !activeMembership.contains(request.candidateId())) {
            return new RequestVoteResponse(currentTerm, false);
        }
        // 如果对方任期更高，更新自己
        if (request.term() > currentTerm) {
            stepDown(request.term());
        }

        // 拒绝条件:
        // 1. 对方任期比自己低
        // 2. 已经投过票给别人
        // 3. 对方的日志不够新 (Raft 安全性: 只有日志至少和投票人一样新才能获票)
        if (request.term() < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }

        boolean canVote = (votedFor == null || votedFor.equals(request.candidateId()));
        boolean logOk = isLogUpToDate(request.lastLogTerm(), request.lastLogIndex());

        if (canVote && logOk) {
            votedFor = request.candidateId();
            lastHeartbeatTime = System.currentTimeMillis();
            log.info("[{}] 投票给 {}, term={}", config.nodeId(), request.candidateId(), currentTerm);
            return new RequestVoteResponse(currentTerm, true);
        }

        return new RequestVoteResponse(currentTerm, false);
    }

    /**
     * 检查候选人的日志是否至少和自己一样新
     */
    private boolean isLogUpToDate(long lastLogTerm, long lastLogIndex) {
        if (lastLogTerm > logStore.lastTerm()) return true;
        if (lastLogTerm < logStore.lastTerm()) return false;
        return lastLogIndex >= logStore.lastIndex();
    }

    /**
     * 成为 Leader
     *
     * Leader 上任后必须做的事:
     *   1. 初始化 nextIndex[] 和 matchIndex[] (每个 Follower 一组)
     *      - nextIndex 初始化为 lastIndex + 1 (下一条要发的日志)
     *      - matchIndex 初始化为 0 (还不知道 Follower 有什么日志)
     *   2. 立即发送空 AppendEntries (心跳) 给所有 Follower
     *      - 确立自己的权威，阻止其他节点发起选举
     *      - 如果 Follower 日志不一致，会在后续心跳中逐步修正
     */
    private void becomeLeader() {
        if (role == NodeRole.LEADER) return; // 防止重复
        role = NodeRole.LEADER;
        leaderId = config.nodeId();
        log.info("[{}] 成为 Leader! term={}", config.nodeId(), currentTerm);

        // 初始化 Leader 的 nextIndex 和 matchIndex
        nextIndex.clear();
        matchIndex.clear();
        for (NodeInfo peer : replicationTargets()) {
            if (!peer.id().equals(config.nodeId())) {
                nextIndex.put(peer.id(), logStore.lastIndex() + 1);
                matchIndex.put(peer.id(), 0L);
            }
        }

        // 新任期空命令使旧任期日志可被间接提交，也能接续中断的配置变更。
        logStore.append(new LogEntry(currentTerm, logStore.lastIndex() + 1, null));
        proposeFinalIfNeeded();
        tryAdvanceCommitIndex();
        // 立即发送心跳，确立权威
        sendHeartbeats();
    }

    // ================================================================
    //  2. Log Replication (日志复制)
    // ================================================================

    /**
     * 客户端提交命令 - 只有 Leader 可以处理
     *
     * 日志复制流程 (Raft 论文 Section 5.3):
     *   1. Leader 将命令追加到本地日志 (未提交)
     *   2. Leader 并行发送 AppendEntries 给所有 Follower
     *   3. 当多数派 (包括 Leader) 都复制了该日志 → Leader 提交
     *   4. Leader 通知 Follower 提交 (通过下一次心跳的 leaderCommit)
     *   5. 所有节点将已提交的日志应用到状态机
     *
     * 注意: 这里返回的 Future 在多数派复制完成后完成，不保证状态机已应用
     */
    public synchronized CompletableFuture<Object> submitCommand(byte[] command) {
        if (!running.get() || role != NodeRole.LEADER) {
            return CompletableFuture.failedFuture(
                    new NotLeaderException(leaderId));
        }

        // 追加日志到本地
        LogEntry entry = new LogEntry(currentTerm, logStore.lastIndex() + 1, command);
        logStore.append(entry);
        log.debug("[{}] 追加日志 index={}, term={}", config.nodeId(), entry.index(), entry.term());

        // 单节点和多节点统一按确认索引推进，失败的 RPC 不计入多数派。
        CompletableFuture<Object> future = new CompletableFuture<>();
        pendingCommits.put(entry.index(), future);
        tryAdvanceCommitIndex();
        sendHeartbeats();
        return future;
    }

    /**
     * 将新日志复制给所有 Follower
     */
    private void tryAdvanceCommitIndex() {
        if (!running.get() || role != NodeRole.LEADER) return;
        // FINAL 只能在 JOINT 提交后追加，届时按新配置计票；否则缩容后旧成员
        // 退出且 Leader 故障时，新 Leader 会因仍等待旧多数派而无法继续提交。
        Membership quorum = activeMembership;
        for (long index = logStore.lastIndex(); index > commitIndex; index--) {
            if (logStore.get(index).term() != currentTerm) continue;
            Set<String> acknowledged = new HashSet<>();
            acknowledged.add(config.nodeId());
            for (NodeInfo peer : quorum.allNodes()) {
                if (matchIndex.getOrDefault(peer.id(), 0L) >= index) acknowledged.add(peer.id());
            }
            if (quorum.hasQuorum(acknowledged)) {
                commitIndex = index;
                applyCommittedEntries();
                proposeFinalIfNeeded();
                break;
            }
        }
    }

    /**
     * 发送 AppendEntries RPC (心跳或日志复制)
     *
     * AppendEntries 的双重作用:
     *   1. 心跳: entries 为空，仅用于维持 Leader 权威
     *   2. 日志复制: entries 包含新日志，发送给 Follower 追加
     *
     * 关键参数:
     *   prevLogIndex/prevLogTerm: 新日志前面的那条日志的索引和任期
     *   → Follower 用它来检查自己的日志是否和 Leader 一致
     *   → 如果不一致，返回 success=false，Leader 回退 nextIndex 重试
     */
    private synchronized void sendHeartbeats() {
        if (!running.get() || role != NodeRole.LEADER) return;
        for (NodeInfo peer : replicationTargets()) {
            if (peer.id().equals(config.nodeId()) || !replicating.add(peer.id())) continue;
            Thread.ofVirtual().start(() -> {
                try { replicateTo(peer); }
                finally { replicating.remove(peer.id()); }
            });
        }
    }

    private List<NodeInfo> replicationTargets() {
        Map<String, NodeInfo> targets = new LinkedHashMap<>();
        committedMembership.allNodes().forEach(n -> targets.put(n.id(), n));
        activeMembership.allNodes().forEach(n -> targets.put(n.id(), n));
        return List.copyOf(targets.values());
    }

    /**
     * 发送心跳, 必要时捎带缺失日志
     *
     * AppendEntries 兼任两个职责: 维持 Leader 权威 (心跳) + 复制日志。
     * 若 follower 的 nextIndex 落后 (例如节点重启后日志为空),
     * 仅等待"新写入触发复制"无法追平, 必须在心跳中补发历史日志:
     *   - nextIndex <= lastIndex: 携带 [nextIndex, lastIndex] 区间的日志
     *   - 否则: 发送纯心跳 (entries 为空)
     */
    private void replicateTo(NodeInfo peer) {
        AppendEntriesRequest request;
        synchronized (this) {
            if (!running.get() || role != NodeRole.LEADER) return;
            long next = Math.min(nextIndex.getOrDefault(peer.id(), 1L), logStore.lastIndex() + 1);
            long previous = next - 1;
            long term = previous == 0 ? 0 : logStore.get(previous).term();
            request = new AppendEntriesRequest(currentTerm, config.nodeId(), previous, term,
                    logStore.getRange(next, logStore.lastIndex()), commitIndex);
        }

        try {
            AppendEntriesResponse response = rpcService.appendEntries(peer, request);

            synchronized (this) {
                if (response.term() > currentTerm) {
                    stepDown(response.term());
                    return;
                }

                if (!running.get() || role != NodeRole.LEADER || request.term() != currentTerm) return;
                if (response.success()) {
                    long matched = request.entries().isEmpty() ? request.prevLogIndex()
                            : request.entries().getLast().index();
                    nextIndex.put(peer.id(), matched + 1);
                    matchIndex.put(peer.id(), matched);
                    tryAdvanceCommitIndex();
                } else {
                    nextIndex.put(peer.id(), Math.max(1, request.prevLogIndex()));
                    matchIndex.put(peer.id(), 0L);
                }
            }
        } catch (Exception e) {
            log.debug("[{}] AppendEntries 失败 -> {}: {}", config.nodeId(), peer.id(), e.getMessage());
        }
    }

    /**
     * 处理 AppendEntries RPC 请求 (作为 Follower 收到 Leader 的日志/心跳)
     *
     * 这是 Raft 中最复杂的方法，包含以下检查:
     *
     * Step 1: 任期检查
     *   如果 Leader 任期 < 当前任期 → 拒绝 (Leader 已过时)
     *
     * Step 2: 认可 Leader
     *   如果 Leader 任期 >= 当前任期 → 接受它作为 Leader
     *   (重置选举超时，因为收到了合法 Leader 的消息)
     *
     * Step 3: 日志匹配检查 (核心!)
     *   检查 prevLogIndex 处的日志是否存在且任期匹配
     *   → 不匹配: 返回 success=false，Leader 会回退 nextIndex 重试
     *   → 匹配: 继续追加新日志
     *
     *   这个机制保证了 "日志一致性":
     *   如果两个日志在相同索引有相同任期，则它们的内容一定相同
     *
     * Step 4: 冲突处理
     *   如果 Follower 在相同索引有不同任期的日志:
     *   → 删除冲突日志及其后面的所有日志 (truncateFrom)
     *   → 然后追加 Leader 的新日志
     *
     * Step 5: 更新提交点
     *   如果 Leader 的 commitIndex 更大 → 更新自己的 commitIndex
     *   → 将新提交的日志应用到状态机
     */
    public synchronized AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        // 如果 Leader 任期比自己低，拒绝
        if (!running.get() || request.term() < currentTerm) {
            return new AppendEntriesResponse(currentTerm, false, config.nodeId(), lastApplied);
        }

        // 认可 Leader
        if (request.term() >= currentTerm) {
            if (request.term() > currentTerm || role != NodeRole.FOLLOWER) {
                stepDown(request.term());
            }
            role = NodeRole.FOLLOWER;
            leaderId = request.leaderId();
            lastHeartbeatTime = System.currentTimeMillis();
        }

        // 日志匹配检查: 检查 prevLogIndex 处的日志是否匹配
        if (request.prevLogIndex() > 0) {
            LogEntry prevEntry = logStore.get(request.prevLogIndex());
            if (prevEntry == null || prevEntry.term() != request.prevLogTerm()) {
                log.debug("[{}] 日志不匹配: prevLogIndex={}, expected term={}, actual={}",
                        config.nodeId(), request.prevLogIndex(), request.prevLogTerm(),
                        prevEntry != null ? prevEntry.term() : "null");
                return new AppendEntriesResponse(currentTerm, false, config.nodeId(), lastApplied);
            }
        }

        // 追加新日志 (处理冲突: 删除冲突日志后追加)
        if (!request.entries().isEmpty()) {
            for (LogEntry entry : request.entries()) {
                LogEntry existing = logStore.get(entry.index());
                if (existing != null && existing.term() != entry.term()) {
                    // 冲突: 删除从这里开始的所有日志
                    if (entry.index() <= commitIndex) {
                        throw new IllegalStateException("不能截断已提交日志");
                    }
                    logStore.truncateFrom(entry.index());
                    existing = null;
                }
                if (existing == null) {
                    logStore.append(entry);
                }
            }
        }

        refreshMembership();
        // 更新 commitIndex
        if (request.leaderCommit() > commitIndex) {
            long matchedThrough = request.prevLogIndex() + request.entries().size();
            commitIndex = Math.max(commitIndex, Math.min(request.leaderCommit(), matchedThrough));
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(currentTerm, true, config.nodeId(), lastApplied);
    }

    // ================================================================
    //  3. 状态机应用
    // ================================================================

    /**
     * 将已提交但未应用的日志应用到状态机
     */
    private void applyCommittedEntries() {
        while (lastApplied < commitIndex) {
            LogEntry entry = logStore.get(lastApplied + 1);
            CompletableFuture<Object> pending = pendingCommits.remove(entry.index());
            try {
                Object result = null;
                if (entry.type() == EntryType.COMMAND) {
                    if (entry.command() != null) result = stateMachine.apply(entry.command());
                } else {
                    committedMembership = MemberCodec.decode(entry.command());
                    log.info("[{}] 应用配置 index={}, type={}, members={}", config.nodeId(),
                            entry.index(), entry.type(), committedMembership);
                    if (entry.type() == EntryType.CONFIG_FINAL && pendingMembership != null) {
                        pendingMembership.complete(committedMembership);
                        pendingMembership = null;
                    }
                }
                lastApplied = entry.index();
                if (pending != null) pending.complete(result);
            } catch (Exception e) {
                if (pending != null) pending.completeExceptionally(e);
                running.set(false);
                failPending(e);
                throw new IllegalStateException("状态机应用失败，停止节点以免跳过日志", e);
            }
        }
        if (role == NodeRole.LEADER && !committedMembership.contains(config.nodeId())) {
            // 退役前尽力将 FINAL 提交点告知新配置节点；失败可由新 Leader 的 no-op 接续。
            for (NodeInfo peer : committedMembership.allNodes()) {
                long last = logStore.lastIndex();
                AppendEntriesRequest notice = new AppendEntriesRequest(currentTerm, config.nodeId(),
                        last, logStore.lastTerm(), List.of(), commitIndex);
                Thread.ofVirtual().start(() -> {
                    try { rpcService.appendEntries(peer, notice); }
                    catch (Exception e) { log.debug("退役通知失败: {}", peer.id()); }
                });
            }
            stepDown(currentTerm);
        }
    }

    /**
     * 配置追加即参与选举，不能延迟至 apply 后才约束多数派。
     * 日志被截断时从 bootstrap 重放配置，恢复到最后一个仍存在的配置。
     * 本项目日志/任期仍在内存中，不提供生产级掉电恢复保证。
     */
    private void refreshMembership() {
        Membership latest = bootstrapMembership;
        configIndex = jointIndex = finalIndex = 0;
        for (LogEntry entry : logStore.getRange(1, logStore.lastIndex())) {
            if (entry.type() == EntryType.COMMAND) continue;
            latest = MemberCodec.decode(entry.command());
            configIndex = entry.index();
            if (entry.type() == EntryType.CONFIG_JOINT) jointIndex = entry.index();
            else finalIndex = entry.index();
        }
        activeMembership = latest;
    }

    /** 一次只变更一个成员；返回值在 FINAL 应用后完成。 */
    public synchronized CompletableFuture<Membership> changeMembership(boolean add, NodeInfo member) {
        if (!running.get() || role != NodeRole.LEADER) {
            return CompletableFuture.failedFuture(new NotLeaderException(leaderId));
        }
        if (activeMembership.isJoint() || configIndex > commitIndex || pendingMembership != null) {
            return CompletableFuture.failedFuture(new IllegalStateException("已有成员变更在途，请等待完成"));
        }
        if (commitIndex == 0 || logStore.get(commitIndex).term() != currentTerm) {
            return CompletableFuture.failedFuture(new IllegalStateException("新 Leader 尚未确认本任期提交点"));
        }
        List<NodeInfo> updated = new ArrayList<>(activeMembership.members());
        if (add) {
            if (activeMembership.contains(member.id())) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("节点已存在: " + member.id()));
            }
            updated.add(member);
        } else {
            if (!updated.removeIf(n -> n.id().equals(member.id()))) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("节点不存在: " + member.id()));
            }
            if (updated.isEmpty()) {
                return CompletableFuture.failedFuture(new IllegalArgumentException("不能移除最后一个成员"));
            }
        }
        Membership joint = Membership.joint(activeMembership.members(), updated);
        CompletableFuture<Membership> future = new CompletableFuture<>();
        pendingMembership = future;
        logStore.append(new LogEntry(currentTerm, logStore.lastIndex() + 1,
                EntryType.CONFIG_JOINT, MemberCodec.encode(joint)));
        refreshMembership();
        for (NodeInfo peer : joint.allNodes()) {
            nextIndex.putIfAbsent(peer.id(), 1L);
            matchIndex.putIfAbsent(peer.id(), 0L);
        }
        tryAdvanceCommitIndex();
        sendHeartbeats();
        return future;
    }

    /** 已提交联合配置后才可追加 FINAL，Leader 更换后也可继续此步骤。 */
    private void proposeFinalIfNeeded() {
        if (role != NodeRole.LEADER || !activeMembership.isJoint() || jointIndex > commitIndex
                || finalIndex > jointIndex) return;
        Membership next = Membership.single(activeMembership.members());
        logStore.append(new LogEntry(currentTerm, logStore.lastIndex() + 1,
                EntryType.CONFIG_FINAL, MemberCodec.encode(next)));
        refreshMembership();
        tryAdvanceCommitIndex();
        sendHeartbeats();
    }

    private void failPending(Throwable cause) {
        pendingCommits.values().forEach(f -> f.completeExceptionally(cause));
        pendingCommits.clear();
        if (pendingMembership != null) {
            pendingMembership.completeExceptionally(cause);
            pendingMembership = null;
        }
    }

    // ================================================================
    //  辅助方法
    // ================================================================

    /**
     * 退回 Follower 状态
     */
    private void stepDown(long newTerm) {
        log.info("[{}] 任期更新 {} -> {}, 退回 Follower", config.nodeId(), currentTerm, newTerm);
        if (newTerm > currentTerm) votedFor = null;
        currentTerm = newTerm;
        role = NodeRole.FOLLOWER;
        leaderId = null;
        lastHeartbeatTime = System.currentTimeMillis();
        failPending(new IllegalStateException("领导权已变更，已追加操作的结果未知，请查询确认"));
    }

    /**
     * 多数派数量
     */
    public Membership getMembership() { return activeMembership; }
    public Membership getCommittedMembership() { return committedMembership; }
    public synchronized boolean isMembershipChanging() {
        return activeMembership.isJoint() || configIndex > commitIndex;
    }

    public synchronized void stop() {
        running.set(false);
        role = NodeRole.FOLLOWER;
        failPending(new IllegalStateException("节点已停止"));
    }

    // ====== Getters ======
    public String getNodeId() { return config.nodeId(); }
    public NodeRole getRole() { return role; }
    public long getCurrentTerm() { return currentTerm; }
    public String getLeaderId() { return leaderId; }
    public long getCommitIndex() { return commitIndex; }
    public long getLastApplied() { return lastApplied; }
    public LogStore getLogStore() { return logStore; }
    public RaftConfig getConfig() { return config; }

    /**
     * 非 Leader 异常
     */
    public static class NotLeaderException extends RuntimeException {
        private final String leaderId;

        public NotLeaderException(String leaderId) {
            super("当前节点不是 Leader, Leader: " + leaderId);
            this.leaderId = leaderId;
        }

        public String getLeaderId() { return leaderId; }
    }
}
