/**
 * <h2>distributed-raft - Raft 共识算法实现</h2>
 *
 * <p>本模块是项目的核心，实现了 Raft 一致性协议的完整算法。</p>
 *
 * <h3>什么是 Raft?</h3>
 * <p>Raft 是一种为了管理复制日志而设计的一致性协议。它保证:</p>
 * <ul>
 *   <li><b>选举安全</b>: 每个任期最多一个 Leader</li>
 *   <li><b>日志匹配</b>: 如果两个日志在某索引处有相同任期，则命令相同</li>
 *   <li><b>Leader 完整性</b>: 已提交的日志不会丢失</li>
 *   <li><b>状态机安全</b>: 所有节点按相同顺序应用日志，产生相同状态</li>
 * </ul>
 *
 * <h3>三大核心流程</h3>
 * <pre>
 * 1. Leader Election (领导者选举)
 *    Follower 超时 → 变为 Candidate → 请求投票 → 获得多数票 → 成为 Leader
 *
 * 2. Log Replication (日志复制)
 *    Client → Leader 追加日志 → AppendEntries 给 Follower → 多数确认 → 提交
 *
 * 3. Safety (安全性)
 *    投票限制: 只有日志至少和投票人一样新才能获得票
 *    日志匹配: prevLogIndex/prevLogTerm 保证日志一致
 * </pre>
 *
 * <h3>阅读顺序</h3>
 * <ol>
 *   <li>{@link com.study.distributed.raft.core.NodeRole} - 三种角色</li>
 *   <li>{@link com.study.distributed.raft.log.LogEntry} - 日志条目</li>
 *   <li>{@link com.study.distributed.raft.rpc.RequestVoteRequest} - 投票 RPC</li>
 *   <li>{@link com.study.distributed.raft.rpc.AppendEntriesRequest} - 日志复制 RPC</li>
 *   <li>{@link com.study.distributed.raft.core.RaftNode} - <b>核心实现 (重点阅读!)</b></li>
 * </ol>
 *
 * <h3>思考题</h3>
 * <ul>
 *   <li>为什么选举超时需要随机化？(提示: 避免分裂投票)</li>
 *   <li>为什么心跳间隔要远小于选举超时？(提示: 避免不必要的选举)</li>
 *   <li>如果一个 Follower 的日志比 Leader 旧很多，如何同步？</li>
 *   <li>网络分区时会发生什么？(提示: 少数派无法选出 Leader)</li>
 * </ul>
 *
 * @see <a href="https://raft.github.io/raft.pdf">Raft 论文</a>
 * @see <a href="https://thesecretlivesofdata.com/raft/">Raft 可视化动画</a>
 */
package com.study.distributed.raft;
