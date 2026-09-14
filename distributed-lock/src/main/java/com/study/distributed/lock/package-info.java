/**
 * <h2>distributed-lock - 分布式锁</h2>
 *
 * <h3>分布式锁 vs 本地锁</h3>
 * <pre>
 * 本地锁 (synchronized/ReentrantLock):
 *   - 只在单个 JVM 内有效
 *   - 无法跨进程互斥
 *
 * 分布式锁:
 *   - 跨多个节点保证互斥
 *   - 需要处理: 网络分区、节点宕机、时钟偏差
 * </pre>
 *
 * <h3>两种实现对比</h3>
 * <table>
 *   <tr><th>特性</th><th>Simple</th><th>Raft</th></tr>
 *   <tr><td>存储</td><td>内存 Map</td><td>Raft KV (共识)</td></tr>
 *   <tr><td>跨节点</td><td>否</td><td>是</td></tr>
 *   <tr><td>容错</td><td>单点故障</td><td>高可用</td></tr>
 *   <tr><td>看门狗</td><td>无</td><td>自动续期</td></tr>
 * </table>
 *
 * <h3>核心问题</h3>
 * <ul>
 *   <li><b>互斥</b>: 同一时刻只有一个持有者</li>
 *   <li><b>死锁</b>: TTL 超时自动释放 + 看门狗续期</li>
 *   <li><b>可重入</b>: 同一线程可重复获取 (重入计数)</li>
 *   <li><b>公平性</b>: 可选的公平/非公平策略</li>
 * </ul>
 */
package com.study.distributed.lock;
