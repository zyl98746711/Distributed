# 分布式系统学习框架 - 复习文档

> 基于 JDK 21 + Spring Boot 3 的手写分布式系统，不依赖任何外部中间件 (Redis/ZK/Nacos)，纯 Java 实现。

---

## 一、整体架构

### 1.1 模块依赖关系

```
① common ──→ ② rpc ──→ ③ raft ──→ ④ kv ──→ ⑤ lock
                              │
                              ├──────────→ ⑥ id (独立)
                              │
                              └──────────→ ⑦ ratelimit (独立)

⑧ node (独立进程封装)  ⑨ manager (控制平面)  ⑩ demo (单 JVM 演示)
```

### 1.2 技术选型

| 选择 | 原因 |
|------|------|
| JDK 21 | Virtual Threads 降低网络编程复杂度；Record 简化消息定义 |
| Spring Boot 3 | HTTP 管理接口、AutoConfiguration |
| 纯手写 | 不依赖 Redis/ZK/Nacos，从原理出发理解本质 |
| Socket + VT | 替代 Netty，降低学习门槛 |
| JSON 序列化 | 可读性好，方便调试 (接口可插拔) |

### 1.3 两种运行模式对比

| 维度 | distributed-demo (单 JVM) | distributed-node (多进程) |
|------|--------------------------|--------------------------|
| 节点通信 | `InMemoryRaftRpcService` 直接方法调用 | `SocketRaftRpcService` 真实 TCP |
| 进程隔离 | 无 (一个进程崩全崩) | 每节点独立 JVM，可单独 kill |
| 故障实验 | 无法模拟机器宕机 | kill/重启节点，观察重选与日志追赶 |
| 客户端入口 | 集中式 `/kv/{nodeId}/...` | 统一经 manager 的 `/manager/kv` |

---

## 二、distributed-common - 公共基础

### 2.1 核心组件

| 组件 | 类型 | 作用 |
|------|------|------|
| `Result<T>` | Record | 统一响应封装 |
| `NodeInfo` | Record | 节点信息模型 (id, host, port) |
| `Serializer` | 接口 | 序列化器抽象 (可插拔策略) |
| `JsonSerializer` | 实现 | 基于 Jackson 的 JSON 序列化 |
| `JdkSerializer` | 实现 | JDK 原生序列化 (对比参考) |

### 2.2 关键设计思想

**为什么序列化器需要接口抽象？**
- 不同场景需要不同策略：JSON 可读性好适合调试，二进制性能好适合生产
- 协议头中携带 `serializerId`，接收端据此选择反序列化器
- 体现了**开闭原则**：新增序列化方式无需修改现有代码

---

## 三、distributed-rpc - 手写 RPC 框架

> 后续所有模块的通信基础，必须首先理解。

### 3.1 核心流程

```
客户端 (RpcClient)                    服务端 (RpcServer)
     │                                      │
     │  1. 构建 RpcRequest                   │
     │  2. 序列化为字节                       │
     │  3. 封装协议帧 (魔数+头部+数据)        │
     │  ──────── TCP Socket ──────────→      │
     │                                      │  4. 解码协议帧
     │                                      │  5. 反序列化为 Request
     │                                      │  6. 反射调用目标方法
     │                                      │  7. 封装 RpcResponse
     │  ←──────── TCP Socket ──────────     │  8. 序列化+编码+发送
     │  9. 解码响应帧                        │
     │  10. 反序列化得到结果                  │
```

### 3.2 自定义二进制协议

```
+--------+--------+--------+--------+
| MAGIC  | VERSION| TYPE   | SER_ID |  固定头部 (4 字节)
| 0xAB   |  0x01  | 1=请求 | 序列化 |
+--------+--------+--------+--------+
|       REQUEST_ID_LENGTH        |  (4 字节, 大端)
+--------------------------------+
|       DATA_LENGTH              |  (4 字节, 大端)
+--------------------------------+
|       REQUEST_ID (变长)         |  如 "req-1"
+--------------------------------+
|       DATA (变长)               |  序列化后的请求/响应体
+--------------------------------+
```

**各字段设计意图：**
- **魔数 (0xAB)**：快速识别协议类型，过滤非法连接
- **版本**：协议升级时兼容旧版本
- **类型**：区分请求 (1) 和响应 (2)
- **序列化方式**：支持多种序列化策略
- **请求 ID**：同一连接多请求并行时，用于匹配响应
- **数据长度**：解决 TCP 粘包问题

### 3.3 TCP 粘包/拆包问题

TCP 是流式协议，没有消息边界：
```
发送: "Hello" + "World"
接收端可能读到: "HelloWorld" 或 "Hel"+"loWorld" 或 "HelloWor"+"ld"
```

**解决方案**：长度前缀帧 —— 头部用 4 字节标明数据体长度，接收端先读头部，再精确读取指定字节数。

### 3.4 Virtual Threads 线程模型

| 角色 | 线程 | 说明 |
|------|------|------|
| 服务端 | `rpc-acceptor` | 循环 accept 连接 |
| 服务端 | `rpc-handler-*` | 每个连接一个 VT，阻塞读不浪费平台线程 |
| 客户端 | `rpc-reader-*` | 每个连接一个 VT，持续读取响应并按 requestId 分发 |

### 3.5 JDK 动态代理

```java
// RpcProxyFactory 核心原理
T proxy = Proxy.newProxyInstance(classLoader, interfaces, handler);
// handler.invoke() 拦截方法调用 → 构建 RpcRequest → 网络发送 → 等待响应
```

**意义**：让远程调用看起来像本地调用，是 Dubbo/gRPC 的核心原理之一。

### 3.6 关键文件速查

| 文件 | 核心职责 |
|------|---------|
| `RpcProtocol` | 协议编解码，理解帧结构 |
| `RpcServer` | accept → 解码 → 反射调用 → 编码响应 |
| `RpcClient` | 编码请求 → 发送 → requestId 匹配响应 |
| `RpcProxyFactory` | JDK 动态代理，方法调用 → RPC 请求 |
| `ServiceRegistry` | 服务注册表接口，理解服务发现原理 |

---

## 四、distributed-raft - Raft 共识算法 (核心)

> 整个项目最核心的模块。建议配合 [Raft 论文](https://raft.github.io/raft.pdf) 和 [可视化演示](https://raft.github.io/) 学习。

### 4.1 三种角色状态转换

```
         选举超时                    获得多数票
FOLLOWER ────────→ CANDATE ────────────────→ LEADER
   ↑                  │                           │
   │                  │ 发现更高任期                │ 发现更高任期
   │                  ↓                           │
   └──────────────────────────────────────────────┘
```

### 4.2 节点状态分类

```java
// ====== 持久化状态 (所有节点, 必须写入磁盘) ======
long currentTerm;        // 当前任期
String votedFor;         // 当前任期投给了谁
LogStore log;            // 日志存储

// ====== 易失状态 (所有节点) ======
NodeRole role;           // FOLLOWER / CANDIDATE / LEADER
long commitIndex;        // 已提交的日志索引
long lastApplied;        // 已应用到状态机的索引

// ====== 易失状态 (Leader 独有, 每次选举后重置) ======
Map<String, Long> nextIndex;   // 每个 Follower 下一条要发的日志索引
Map<String, Long> matchIndex;  // 每个 Follower 已匹配的最高日志索引
```

### 4.3 Leader 选举

**触发条件**：Follower 在 `electionTimeout` 内没收到心跳

**选举流程**：
1. 角色变为 Candidate
2. `currentTerm++` (开始新任期)
3. 投票给自己 (`votedFor = self`)
4. 向所有节点发送 `RequestVote RPC`
5. 收到多数票 → 成为 Leader
6. 发现更高任期 → 退回 Follower

**选举超时随机化 (避免活锁的关键!)**：
```
固定超时的问题:
  3 节点 A/B/C，超时都是 3s
  → 同时变为 Candidate → 各得 1 票 → 无人过半 → 反复循环 (活锁)

随机化后:
  A 超时 2.1s, B 超时 3.5s, C 超时 4.2s
  → A 先发起选举，B/C 还是 Follower，投票给 A
  → A 获得 3 票，当选 Leader
```

### 4.4 投票决策规则

一个节点在一个任期内最多投一票，拒绝条件 (满足任一)：
1. 候选人任期 < 当前任期
2. 本节点在本任期已投过票给别人
3. **候选人的日志不够新** (安全性关键!)

**日志完整性检查**：如果候选人最后一条日志的任期比自己小，或任期相同但索引更小，说明日志不够完整，不能成为 Leader (否则已提交的数据可能丢失)。

### 4.5 日志复制

```
Client 请求 → Leader 追加日志到本地 (未提交)
           → Leader 发送 AppendEntries 给所有 Follower
           → 多数派确认后 → commitIndex 推进
           → 应用到状态机 (StateMachine.apply)
```

**AppendEntries 的双重作用**：
1. **心跳**：entries 为空，仅维持 Leader 权威
2. **日志复制**：entries 包含新日志，捎带给 Follower

**日志匹配原则**：
- 如果两个日志在相同索引有相同任期，则它们的内容一定相同
- Follower 检查 `prevLogIndex` 处的日志是否匹配
- 不匹配 → 返回 `success=false`，Leader 回退 `nextIndex` 重试

**冲突处理**：
- Follower 在相同索引有不同任期的日志 → 删除冲突日志及其后面所有日志
- 然后追加 Leader 的新日志
- **已提交日志不能被截断**

### 4.6 Leader 上任后的操作

1. 初始化 `nextIndex[]` 和 `matchIndex[]`
2. 追加一条**空命令日志** (no-op)：使旧任期日志可被间接提交
3. 立即发送心跳，确立权威

### 4.7 Joint Consensus (动态成员变更)

**两阶段变更**：
```
CONFIG_JOINT(C_old, C_new)  →  提交后  →  CONFIG_FINAL(C_new)
```

**联合期特点**：
- 选举和日志提交要求旧、新配置**各自**的多数派
- `hasQuorum()` 实现：`majority(members) && (!isJoint() || majority(oldMembers))`
- 一次只变更一个成员；拒绝并发变更

**配置日志**：
- `CONFIG_JOINT`：联合配置，参与选举
- `CONFIG_FINAL`：最终配置，提交后切换为新配置多数派
- 配置追加即参与选举，日志冲突截断后从 bootstrap 重放配置恢复

### 4.8 关键文件速查

| 文件 | 核心职责 |
|------|---------|
| `RaftNode` | **核心中的核心** - 选举、复制、安全性的完整实现 |
| `LogEntry` / `EntryType` | 日志条目 Record (term + index + type + command) |
| `Membership` / `MemberCodec` | 不可变成员集、双多数派判断及配置日志编码 |
| `LogStore` / `InMemoryLogStore` | 日志存储接口和内存实现 |
| `RequestVoteRequest/Response` | 投票 RPC 消息 |
| `AppendEntriesRequest/Response` | 日志复制/心跳 RPC 消息 |
| `StateMachine` | 状态机接口 |
| `RaftConfig` | 配置 (选举超时随机化是关键!) |
| `SocketRaftRpcService` | 真实 TCP 长连接 + requestId 匹配 + 超时重连 |

---

## 五、distributed-kv - 分布式 KV 存储

> 用 Raft 共识构建真正可用的分布式存储，验证 Raft 的正确性。

### 5.1 架构

```
              ┌──────────────┐
  PUT/DELETE  │  RaftNode    │  所有写操作走 Raft 共识
 ───────────→ │  submitCmd() │
              └──────┬───────┘
                     │ 日志提交后
                     ↓
              ┌──────────────┐
              │ KVStateMachine│  确定性状态机
              │  apply(cmd)  │
              └──────┬───────┘
                     ↓
              ┌──────────────┐
              │  MemoryKV    │  ConcurrentHashMap
              │   Store      │
              └──────────────┘
```

### 5.2 状态机确定性

**核心原则**：相同的命令序列一定产生相同的状态。

```java
// KVStateMachine.apply() 实现
switch (cmd.type()) {
    case PUT    → kvStore.put(key, value) → "OK"
    case GET    → kvStore.get(key)
    case DELETE → kvStore.delete(key) → "OK" / "NOT_FOUND"
}
```

这是 Raft 保证所有节点状态一致的关键 —— 只要日志顺序相同，apply 后状态一定相同。

### 5.3 读写策略

| 操作 | 路径 | 一致性 |
|------|------|--------|
| 写 (PUT/DELETE) | 必须走 Raft 共识 | 强一致 |
| 读 (GET) | 直接读本地状态机 | 最终一致 |

**写转发**：客户端可连任意节点，非 Leader 节点将写请求转发到 Leader (`X-Raft-Forwarded` 头防循环)。

---

## 六、distributed-lock - 分布式锁

### 6.1 两种实现对比

| 特性 | SimpleDistributedLock | RaftDistributedLock |
|------|----------------------|---------------------|
| 实现方式 | `ConcurrentHashMap` + TTL | 基于 Raft KV 共识 |
| 跨节点 | 不支持 | 支持 |
| 容错 | 单点故障 | Raft 保证高可用 |
| 可重入 | 支持 (holdCount) | 支持 (ownerId 检查) |
| 看门狗 | 无 | 支持自动续期 |
| 适用场景 | 单机学习 | 生产级分布式锁 |

### 6.2 SimpleDistributedLock 实现要点

```java
// 全局锁表
Map<String, LockInfo> LOCKS = new ConcurrentHashMap<>();

// 可重入: 同一 ownerId 再次获取 → holdCount++
// TTL 过期: System.currentTimeMillis() - acquireTime > ttlMs → 强制释放
// 原子获取: putIfAbsent() 保证互斥
```

### 6.3 RaftDistributedLock 实现要点

```java
// 锁 = KV 中的特殊 key
lock(key)   → kvService.put("__lock__:" + key, ownerId)  // CAS: 仅当不存在时
unlock(key) → kvService.delete("__lock__:" + key)

// 验证获取: 写入后再读一次，确认值是自己
// 看门狗: 每 TTL/3 续期一次，防止业务未完成锁就过期
```

### 6.4 看门狗机制

```
为什么需要看门狗?
  假设锁 TTL=30s，业务执行了 60s
  → 30s 时锁过期，其他线程获取了锁
  → 两个线程同时持有锁，互斥被破坏!

看门狗方案:
  后台线程每 TTL/3 检查一次，如果仍持有锁就续期
  → 只要进程存活，锁就不会过期
  → 进程崩溃时，看门狗线程也死，锁自然过期 (容错)
```

### 6.5 分布式锁核心问题

1. **互斥**：同一时刻只有一个线程持有锁
2. **死锁**：TTL 兜底 + 看门狗续期
3. **容错**：Raft 保证高可用，节点故障可重选
4. **可重入**：同一线程可重复获取同一把锁

---

## 七、distributed-id - 分布式 ID

### 7.1 雪花算法 (Snowflake)

**ID 结构 (64 bit)**：
```
 0 | 41位时间戳 | 10位机器ID | 12位序列号
   | (69年)     | (1024节点) | (4096/毫秒/节点)
```

**位运算实现**：
```java
long id = ((timestamp - EPOCH) << 22)   // 时间戳左移 22 位
        | (workerId << 12)               // 机器ID左移 12 位
        | sequence;                       // 序列号

// 解析 (反向移位)
timestamp = (id >> 22) + EPOCH;
workerId  = (id >> 12) & 0x3FF;          // & 10位掩码
sequence  = id & 0xFFF;                   // & 12位掩码
```

**序列号溢出处理**：
```java
sequence = (sequence + 1) & MAX_SEQUENCE;  // & 4095 等价于 % 4096
if (sequence == 0) {
    // 当前毫秒用完，等待下一毫秒
    currentTimestamp = waitNextMillis(lastTimestamp);
}
```

**时钟回拨处理**：
| 回拨幅度 | 策略 |
|----------|------|
| ≤ 5ms | 等待 2 倍时间让时钟追上 |
| > 5ms | 直接抛异常 (等待太久) |

**生产环境更好方案**：使用单调递增时钟 `clock_gettime(CLOCK_MONOTONIC)`。

### 7.2 号段模式

```
外部存储 (DB/文件) ──批量获取──→ [号段 Buffer] ──逐个分配──→ ID
                                  ↓ 用到 20% 时
                                异步预加载下一个号段
```

**双 Buffer 预加载**：
- 当前号段用到 20% 时，异步加载下一个号段
- 即使外部存储不可用，也能用本地缓存继续生成一段时间
- 用 `AtomicLong` 保证线程安全的 ID 分配

---

## 八、distributed-ratelimit - 分布式限流

### 8.1 四种算法对比

| 算法 | 原理 | 优点 | 缺点 | 适用场景 |
|------|------|------|------|---------|
| **固定窗口** | 时间分窗口计数 | 简单，内存小 | 边界 2x 流量 | 粗粒度限流 |
| **滑动窗口** | 记录每个请求时间戳 | 精确 | 内存占用大 | 精确限流 |
| **令牌桶** | 固定速率放令牌，请求取令牌 | 允许突发 | - | API 网关 |
| **漏桶** | 固定速率处理请求 | 恒定速率 | 无法突发 | DB 写入 |

### 8.2 固定窗口 - 边界问题

```
|←─── 窗口1 ───→|←─── 窗口2 ───→|
0s             1s              2s
           ↑0.9s↑1.1s↑
           100次 100次  ← 0.2s 内通过 200 次!
```

### 8.3 令牌桶 vs 漏桶

```
令牌桶: 允许突发              漏桶: 恒定速率
  ↓ ↓↓ ↓    ↓↓               ↓ ↓ ↓ ↓ ↓ ↓ ↓
  ╔═══════╗                   ╔═══════╗
  ║ Token ║ → 有就用          ║ Water ║ → 固定流出
  ║ Bucket║                   ║ Bucket║
  ╚═══════╝                   ╚═══════╝

空闲 10 秒后突然来 10 个请求:
- 令牌桶: 10 个全部通过 (桶中有 10 个令牌)
- 漏桶:   只有 1 个通过，其余排队/拒绝
```

### 8.4 实现细节

**令牌桶**：
```java
// 根据时间流逝计算应补充的令牌数
long elapsed = now - lastRefillTime;
long newTokens = elapsed / refillIntervalMs;
tokens = Math.min(maxTokens, tokens + newTokens);
```

**滑动窗口**：
```java
// 用 Deque 存储时间戳，清除窗口外的
while (!timestamps.isEmpty() && timestamps.peekFirst() <= windowStart) {
    timestamps.pollFirst();
}
```

### 8.5 AOP 注解驱动

```java
@RateLimit(key = "api", maxRequests = 100, windowMs = 1000)
public String api() { ... }
```

通过 `RateLimitAspect` 切面拦截，自动执行限流检查。

---

## 九、distributed-node - 独立节点进程

> 把每个 Raft 节点放进独立 JVM 进程，通过真实 TCP 通信 —— 真正的"分布式"。

### 9.1 架构图

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  进程 node-1  │     │  进程 node-2  │     │  进程 node-3  │
│  RaftNode    │     │  RaftNode    │     │  RaftNode    │
│  KV 状态机    │     │  KV 状态机    │     │  KV 状态机    │
│  Socket RPC  │     │  Socket RPC  │     │  Socket RPC  │
│  RPC :9001   │     │  RPC :9002   │     │  RPC :9003   │
│  HTTP :8001  │     │  HTTP :8002  │     │  HTTP :8003  │
└───────┼──────┘     └───────┼──────┘     └───────┼──────┘
        └──────────────── TCP 真实网络 ────────────────┘
```

### 9.2 SocketRaftRpcService 设计

**线程模型**：
```
入站:
  raft-rpc-acceptor    循环 accept 新连接
  raft-rpc-handler-*   每个入站连接一个 VT: 读请求 → RaftNode 处理 → 写响应

出站:
  raft-rpc-reader-*    每个出站连接一个 VT: 持续读响应，按 requestId 匹配
  (调用方 VT)           发送请求后阻塞等待响应 (带超时)
```

**可靠性处理**：
- 等待响应带超时 (2000ms)，超时后清理连接缓存，下次自动重连
- 连接断开时，快速失败该连接上所有等待中的请求
- 目标节点被 kill 后，发送抛异常，由 RaftNode 捕获并容忍

**RaftProtocol 帧格式**：
- 长度前缀帧 + 消息类型标签，解决粘包
- 4 字节长度 + 1 字节类型 + UUID requestId + JSON payload

---

## 十、distributed-manager - 控制平面

### 10.1 架构角色

```
其他服务 ── HTTP :7000 ──> manager（控制平面，不参与投票）
                          ├─ 配置清单 / 本机进程启停
                          └─ /manager/kv 自动发现并路由 Leader
                                      │
                     node-1 <── Raft TCP ──> node-2 <──> node-N
                                数据平面 / 联合共识
```

### 10.2 核心组件

| 组件 | 职责 |
|------|------|
| `ManagerProperties` | 节点 jar 路径、预置清单、auto-start |
| `NodeProcessManager` | 进程生命周期管理 (启动/探活/停止) |
| `LeaderLocator` | 每 2 秒探测 Leader；缓存失效时重新发现 |
| `NodeHttpClient` | 有限超时，严格编码 URI，防止无限等待 |
| `ManagerController` | 集群状态、串行扩缩容、进程控制、KV 网关 |

### 10.3 设计边界

- 仅支持 `127.0.0.1`/`localhost` 进程编排
- manager 不参与 Raft 投票
- 一次变更一个成员；拒绝并发变更及移除最后一个成员
- 网关读路由至 Leader，但**不是线性一致读** (无 ReadIndex/租约验证)
- Raft 日志/任期/KV 在内存中，不提供掉电安全保证

---

## 十一、CAP 理论分析

### 11.1 CAP 定理回顾

```
CAP 定理: 分布式系统最多同时满足两项

  C (Consistency)  一致性 — 所有节点在同一时刻看到相同数据
  A (Availability) 可用性 — 每个请求都能得到非错误响应 (不保证最新数据)
  P (Partition)    分区容错 — 网络分区时系统仍能运行

  P 是分布式系统的必然条件 (网络不可靠)，所以实际选择是:
  ┌─────────┬──────────────────────────────────────────┐
  │  CP     │ 保证一致性，分区时牺牲可用性 (拒绝部分请求) │
  │         │ 代表: ZooKeeper, Redis Cluster, HBase     │
  ├─────────┼──────────────────────────────────────────┤
  │  AP     │ 保证可用性，分区时牺牲一致性 (返回旧数据)   │
  │         │ 代表: Cassandra, DynamoDB, DNS            │
  └─────────┴──────────────────────────────────────────┘
```

### 11.2 本项目的 CAP 定位

**本项目是 CP 系统** — 基于 Raft 的一致性保证。

```
                    C (一致性)
                    ▲
                    │
         ZooKeeper ●│
    Redis Cluster ●│
       本项目 Raft ●│  ← CP: 多数派确认才提交
                    │
  ──────────────────┼──────────────────► A (可用性)
                    │
              DNS ● │
        Cassandra ● │  ← AP: 总是可读写
           DNS ●    │
                    │
```

### 11.3 各组件的 CAP 分析

| 组件 | CAP 选择 | 具体表现 |
|------|---------|----------|
| **Raft 日志复制** | CP | 必须多数派确认才 commitIndex 推进，少数派节点不可用时仍能工作 (只要多数派存活) |
| **Leader 选举** | CP | 必须获得多数票才能成为 Leader，网络分区时小分区无法选出 Leader |
| **KV 读操作** | AP (当前实现) | 直接读本地状态机，不验证 Leader 租约，**不是线性一致读** |
| **KV 写操作** | CP | 必须转发到 Leader，经 Raft 共识后才返回 |
| **ServiceRegistry** | AP | `InMemoryServiceRegistry` 无持久化，各节点本地视图可能不一致 |
| **LeaderLocator** | CP (最终) | 每 2 秒轮询探测 Leader，缓存失效时重新发现，有短暂不一致窗口 |
| **分布式锁 (Simple)** | AP | 单节点内存，无共识，可能因 TTL 过期导致两个持有者 |
| **分布式锁 (Raft)** | CP | 写操作走 Raft 共识，保证互斥性 |

### 11.4 网络分区时的行为

```
场景: 3 节点集群 (node-1, node-2, node-3)，node-1 是 Leader

  ┌─────────┐          ┌─────────┐
  │ node-1  │ ╳╳╳╳╳╳╳ │ node-2  │
  │ LEADER  │ 网络分区  │ node-3  │
  │ term=5  │          │ 新选举   │
  └─────────┘          └─────────┘
    大分区 (2/3)           小分区 (1/3)

大分区侧 (node-2 + node-3):
  ✓ 有 2/3 多数派
  ✓ 可以选出新 Leader
  ✓ 可以继续接受读写
  ✓ 保持一致性

小分区侧 (node-1):
  ✗ 只有 1/3，无法获得多数票
  ✗ 无法提交新日志
  ✗ 客户端写请求被拒绝 (NotLeaderException)
  → 牺牲了可用性，但保证不会写入分裂数据
```

**结论**: Raft 在网络分区时，多数派侧保持 C+P，少数派侧牺牲 A 保证 C。

### 11.5 本项目中的一致性级别

```
强一致 (线性一致) ← ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ → 最终一致

  Raft 写操作                    KV 读操作
  (Leader + 多数派确认)          (直接读本地状态机)
       │                              │
       ▼                              ▼
  ┌──────────┐                  ┌──────────┐
  │ 强一致    │                  │ 最终一致  │
  │ 提交后所有│                  │ 等日志复制│
  │ 节点一致  │                  │ 到本节点后│
  └──────────┘                  │ 才一致    │
                                └──────────┘
```

**改进方向**:
- 读操作走 Leader 可提升为线性一致 (ReadIndex/LeaseRead)
- 当前 `LeaderLocator` 的 2 秒轮询间隔是已知的一致性窗口

---

## 十二、Consul 模式改造方案

### 12.1 Consul 核心概念

```
Consul = 服务发现 + 健康检查 + KV 存储 + 分布式配置

核心组件:
  ┌──────────────────────────────────────────────────┐
  │                  Consul Cluster                   │
  │  (Server 节点, Raft 共识, 3 或 5 个)              │
  │                                                   │
  │  ┌──────────┐  ┌──────────┐  ┌──────────┐       │
  │  │ Server-1 │  │ Server-2 │  │ Server-3 │       │
  │  │ (Leader) │  │ Follower │  │ Follower │       │
  │  └──────────┘  └──────────┘  └──────────┘       │
  └──────────────────────────────────────────────────┘
         ▲              ▲              ▲
         │              │              │
  ┌──────┴───┐   ┌──────┴───┐   ┌──────┴───┐
  │ Client-1 │   │ Client-2 │   │ Client-3 │
  │ (Agent)  │   │ (Agent)  │   │ (Agent)  │
  └──────────┘   └──────────┘   └──────────┘
       ▲              ▲              ▲
       │              │              │
  ┌────┴─────┐   ┌────┴─────┐   ┌────┴─────┐
  │ Service A│   │ Service B│   │ Service C│
  └──────────┘   └──────────┘   └──────────┘
```

**Consul 内部也用 Raft**，但它把 Raft 封装成了开箱即用的服务:
- **Catalog**: 服务注册与发现
- **Health Check**: 自动摘除不健康节点
- **KV Store**: 强一致 KV 存储
- **Session**: 分布式锁/会话管理 (基于 TTL + 心跳)

### 12.2 当前架构 vs Consul 架构对比

```
当前架构 (手写 Raft):
  ┌──────────────────────────────────────────────┐
  │ 应用自己实现 Raft + 服务发现 + KV + 锁        │
  │                                               │
  │  distributed-manager  ←→  distributed-node    │
  │  (进程编排 + Leader 探测)    (Raft 节点 + KV)  │
  │                                               │
  │  InMemoryServiceRegistry (无持久化, 无健康检查) │
  └──────────────────────────────────────────────┘

Consul 架构 (使用 Consul):
  ┌──────────────────────────────────────────────┐
  │  Consul 集群 (独立部署, 负责共识/发现/健康)    │
  │                                               │
  │  ┌─────────────────┐   ┌──────────────────┐  │
  │  │ Consul Server×3  │   │ Consul KV (强一致)│  │
  │  │ (Raft 共识)      │   │ (底层也是 Raft)   │  │
  │  └─────────────────┘   └──────────────────┘  │
  │         ▲                    ▲                 │
  │         │                    │                 │
  │  ┌──────┴────────────────────┴──────────┐     │
  │  │ 应用节点 (无状态, 只注册服务)          │     │
  │  │  node-1 / node-2 / node-3            │     │
  │  └──────────────────────────────────────┘     │
  └──────────────────────────────────────────────┘
```

### 12.3 改造点一: 服务注册与发现

**当前实现** (`InMemoryServiceRegistry`):
```java
// 内存注册表，无持久化，无健康检查
Map<String, List<NodeInfo>> registry = new ConcurrentHashMap<>();
void register(String serviceName, NodeInfo nodeInfo);
List<NodeInfo> lookup(String serviceName);
```

**改造为 Consul 服务发现**:
```java
public class ConsulServiceRegistry implements ServiceRegistry {

    private final ConsulClient consul;  // com.ecwid.consul / consul-client

    @Override
    public void register(String serviceName, NodeInfo nodeInfo) {
        // 注册到 Consul Catalog
        NewService service = new NewService();
        service.setId(nodeInfo.id());
        service.setName(serviceName);
        service.setAddress(nodeInfo.host());
        service.setPort(nodeInfo.port());
        // 添加健康检查: HTTP GET /health 每 3 秒
        service.setCheck(buildHttpCheck(nodeInfo));
        consul.agentServiceRegister(service);
    }

    @Override
    public void unregister(String serviceName, NodeInfo nodeInfo) {
        consul.agentServiceDeregister(nodeInfo.id());
    }

    @Override
    public List<NodeInfo> lookup(String serviceName) {
        // Consul 自动过滤不健康节点
        List<HealthService> services =
            consul.healthServices(serviceName, true, null).getValue();
        return services.stream()
            .map(s -> new NodeInfo(
                s.getService().getId(),
                s.getService().getAddress(),
                s.getService().getPort()))
            .toList();
    }
}
```

**改造收益**:
| 维度 | 当前 | Consul |
|------|------|--------|
| 持久化 | 无 (进程重启丢失) | Consul 持久化到 Raft log |
| 健康检查 | 无 | 自动 HTTP/TCP 检查，不健康自动摘除 |
| 变更通知 | 轮询 | Watch/Blocking Query 实时推送 |
| 多数据中心 | 不支持 | 原生支持 |

### 12.4 改造点二: Leader 发现

**当前实现** (`LeaderLocator`):
```java
// 每 2 秒轮询所有节点，逐个 HTTP 请求查状态
@Scheduled(fixedDelay = 2000)
public synchronized void refresh() {
    for (ManagedNode node : processes.snapshot()) {
        JsonNode status = http.status(node.definition());
        if ("LEADER".equals(status.path("role").asText())) {
            leaderId = node.definition().id();
        }
    }
}
```

**改造方案 A: Consul KV + 主动注册 Leader**
```java
// Raft 节点当选 Leader 后，主动写入 Consul KV
public void becomeLeader() {
    // ... 原有 Raft 逻辑 ...

    // 写入 Consul KV，其他客户端通过 Consul 读取
    consul.kvPut("cluster/leader", nodeId);
    consul.kvPut("cluster/leader-term", String.valueOf(currentTerm));
}

// 客户端通过 Consul 获取 Leader (支持 Blocking Query)
public String getLeader() {
    Value v = consul.kvGetValue("cluster/leader");
    return v.getValueAsString();
}
```

**改造方案 B: Consul Session + Lock (替代手写 Leader 选举)**
```java
// 完全用 Consul Session 做 Leader 选举
// 不再需要手写 Raft 选举 (适合不需要强一致日志复制的场景)

Session session = consul.sessionCreate(
    Session.builder()
        .setName("cluster-leader")
        .setTtl("15s")       // TTL 15 秒
        .setBehavior(Session.Behavior.DELETE)  // 释放时删除锁
        .build(), null).getValue();

// 尝试获取锁
Lock lock = consul.lock("cluster/leader-election", session);
boolean acquired = lock.acquire();
if (acquired) {
    // 我是 Leader，执行业务逻辑
    // Consul 自动续期 Session TTL
}
// 进程崩溃 → Session TTL 过期 → 锁自动释放 → 其他节点竞选
```

### 12.5 改造点三: KV 存储

**当前**: 手写 Raft + KVStateMachine + MemoryKVStore

**改造为 Consul KV**:
```java
// Consul KV 本身就是强一致的 (底层 Raft)
// 不再需要手写状态机

public class ConsulKVStore implements KVStore {

    private final ConsulClient consul;

    @Override
    public void put(String key, String value) {
        consul.kvPut(key, value);  // 强一致写入
    }

    @Override
    public String get(String key) {
        Value v = consul.kvGetValue(key);
        return v != null ? v.getValueAsString() : null;
    }

    @Override
    public String delete(String key) {
        String old = get(key);
        consul.deleteKVValues(key);
        return old;
    }
}
```

**对比**:
| 维度 | 当前 (手写 Raft KV) | Consul KV |
|------|---------------------|----------|
| 一致性 | 强一致 (Raft 多数派) | 强一致 (Raft 多数派) |
| 持久化 | 内存 (重启丢失) | 磁盘 (Raft log 持久化) |
| 事务 | 不支持 | 支持 CAS (Check-And-Set) |
| 事务2 | - | 支持多 Key 事务 (txn) |
| Watch | 不支持 | Blocking Query 实时通知 |
| 学习价值 | 高 (理解 Raft 原理) | 低 (黑盒使用) |

### 12.6 改造点四: 分布式锁

**当前**: `RaftDistributedLock` (手写 KV + 看门狗)

**改造为 Consul Lock**:
```java
// Consul 原生分布式锁 (基于 Session + KV)
// 底层: Session TTL + KV CAS 操作

Lock lock = consul.lock("locks/" + lockName);
boolean acquired = lock.acquire();  // 阻塞直到获取

// 自动续期: Consul Agent 自动维护 Session TTL
// 进程崩溃: Session TTL 过期 → 锁自动释放
// 无需手写看门狗!

// 释放
lock.release();
```

**对比**:
| 维度 | 当前 (Raft KV 锁) | Consul Lock |
|------|-------------------|-------------|
| 实现 | 手写 KV CAS + 看门狗 | Session + KV CAS (内置) |
| 续期 | 自己实现看门狗线程 | Consul Agent 自动续期 |
| 公平性 | 无 (谁先抢到算谁的) | 可选 FIFO (lock-delay) |
| 可重入 | 手动检查 ownerId | 不支持 (需自行包装) |

### 12.7 改造点五: Manager 角色变化

```
当前架构:
  manager = 进程编排 + Leader 发现 + 统一网关
  (manager 自己管理节点生命周期)

Consul 架构:
  manager 大幅简化:
  ┌──────────────────────────────────────────┐
  │  manager (简化后)                         │
  │  ├─ 进程编排 (保留 ProcessBuilder)        │
  │  ├─ 统一网关 (保留, 但从 Consul 查 Leader)│
  │  └─ 不再需要 LeaderLocator 轮询          │
  │                                          │
  │  新增:                                    │
  │  ├─ 节点启动时自动注册到 Consul            │
  │  ├─ 健康检查由 Consul 负责               │
  │  └─ Leader 变更通过 Consul Watch 推送     │
  └──────────────────────────────────────────┘
```

### 12.8 改造全景图

```
改造前 (全手写):
  ┌────────────────────────────────────────────────────┐
  │  manager ──→ node-1/2/3 (手写 Raft + KV + 锁)     │
  │     │                                              │
  │     ├─ InMemoryServiceRegistry (无持久化)           │
  │     ├─ LeaderLocator (2s 轮询)                    │
  │     └─ MemoryKVStore (内存, 重启丢失)              │
  └────────────────────────────────────────────────────┘

改造后 (Consul 模式):
  ┌────────────────────────────────────────────────────┐
  │              Consul Cluster (3 Server)              │
  │              ├─ Catalog (服务注册/发现)              │
  │              ├─ KV Store (强一致 KV)                │
  │              ├─ Session (分布式锁/会话)              │
  │              └─ Health Check (健康检查)              │
  └───────────────────────┬────────────────────────────┘
                          │
  ┌───────────────────────┼────────────────────────────┐
  │  manager              │                            │
  │  ├─ 进程编排 (保留)    │                            │
  │  ├─ 统一网关           │                            │
  │  │   └─ 从 Consul 查 Leader (Blocking Query)       │
  │  └─ 不再轮询           │                            │
  │                       │                            │
  │  node-1/2/3 (无状态化) │                            │
  │  ├─ 启动时注册 Consul  │                            │
  │  ├─ KV → Consul KV    │                            │
  │  ├─ 锁 → Consul Lock  │                            │
  │  └─ 崩溃 → Consul 自动摘除                          │
  └────────────────────────────────────────────────────┘
```

### 12.9 改造步骤总结

| 步骤 | 改造内容 | 涉及模块 | 难度 |
|------|---------|---------|------|
| 1 | 引入 Consul 客户端依赖 | pom.xml | 低 |
| 2 | 实现 `ConsulServiceRegistry` 替代 `InMemoryServiceRegistry` | distributed-rpc | 低 |
| 3 | 节点启动时注册 Consul + 健康检查端点 | distributed-node | 低 |
| 4 | `LeaderLocator` 改为 Consul KV 读取 + Watch | distributed-manager | 中 |
| 5 | KV 存储改为 Consul KV (可选, 会丧失学习价值) | distributed-kv | 中 |
| 6 | 分布式锁改为 Consul Lock | distributed-lock | 中 |
| 7 | 移除手写 Raft 选举 (如果完全迁移) | distributed-raft | 高 (等于重写) |

### 12.10 Consul 模式的 CAP 分析

| 组件 | CAP 变化 | 说明 |
|------|---------|------|
| 服务发现 | AP → AP | Consul 健康检查保证最终一致，但 Blocking Query 有短暂延迟 |
| KV 存储 | CP → CP | Consul KV 底层也是 Raft，一致性保证不变 |
| 分布式锁 | CP → CP | Consul Session 基于 Raft，锁互斥性保证 |
| Leader 选举 | CP → CP | 仍然依赖 Raft 共识 (Consul 内部) |

**本质区别**: 从「自己实现 Raft」变成「使用 Consul 提供的 Raft 服务」，CAP 特性本质相同，只是实现从代码变成了外部依赖。

### 12.11 何时选择 Consul 模式?

| 场景 | 推荐方案 |
|------|----------|
| 学习 Raft 原理 | 保持当前手写实现 |
| 微服务注册发现 | Consul / Nacos / Eureka |
| 分布式锁 (生产) | Consul Lock / Redis RedLock / ZK |
| 配置中心 | Consul KV / Nacos / Apollo |
| 需要多数据中心 | Consul (原生支持) |
| 需要强一致 KV + 服务发现 | Consul (一体化方案) |

---

## 十三、核心知识点索引

### 网络通信

| 知识点 | 所在模块 | 关键文件 |
|--------|---------|---------|
| TCP 粘包/拆包 | rpc | `RpcProtocol` |
| 长度前缀帧 | raft | `RaftProtocol` |
| 自定义二进制协议 | rpc | `RpcProtocol` |
| JDK 动态代理 | rpc | `RpcProxyFactory` |
| Virtual Threads | rpc, raft | `RpcServer`, `SocketRaftRpcService` |
| requestId 匹配响应 | rpc, raft | `RpcClient`, `SocketRaftRpcService` |

### Raft 共识

| 知识点 | 关键方法/文件 |
|--------|-------------|
| Leader 选举 | `RaftNode.startElection()` |
| 日志复制 | `RaftNode.submitCommand()` |
| 日志匹配 (安全性) | `RaftNode.handleAppendEntries()` |
| 投票限制 (安全性) | `RaftNode.handleRequestVote()` |
| 选举超时随机化 | `RaftNode.randomElectionTimeout()` |
| 心跳捎带日志 | `RaftNode.replicateTo()` |
| Joint Consensus 与双多数派 | `Membership`, `RaftNode.changeMembership()` |
| 控制平面与数据平面分离 | `ManagerController`, `NodeMembershipController` |

### 应用层

| 知识点 | 所在模块 | 关键文件 |
|--------|---------|---------|
| 状态机确定性 | kv | `KVStateMachine` |
| 锁的 TTL + 看门狗 | lock | `RaftDistributedLock` |
| 可重入锁 | lock | `SimpleDistributedLock` |
| 雪花算法位运算 | id | `SnowflakeIdGenerator` |
| 时钟回拨处理 | id | `SnowflakeIdGenerator.nextId()` |
| 号段双 Buffer | id | `SegmentIdGenerator` |
| 固定窗口边界问题 | ratelimit | `FixedWindowLimiter` |
| 令牌桶 vs 漏桶 | ratelimit | `TokenBucketLimiter` vs `LeakyBucketLimiter` |

### Java 特性

| 特性 | 使用场景 |
|------|----------|
| Record (JDK 16+) | 所有消息/模型类 (`LogEntry`, `RpcRequest`, `NodeInfo` 等) |
| Virtual Threads (JDK 21) | 网络通信，每个连接/请求一个虚拟线程 |
| `CompletableFuture` | 异步提交、pending 匹配 |
| `ConcurrentHashMap` | 线程安全的映射存储 |
| `synchronized` | RaftNode 状态变更的原子性 |

---

## 十四、快速命令参考

```bash
# 编译
mvn clean compile

# 单 JVM 演示模式
cd distributed-demo && mvn spring-boot:run

# 真实分布式 (manager 管理多进程)
mvn clean package
java -jar distributed-manager/target/distributed-manager-1.0.0-SNAPSHOT.jar

# 统一网关读写 (端口 7000)
curl http://127.0.0.1:7000/manager/cluster
curl -X PUT "http://127.0.0.1:7000/manager/kv/name?value=raft"
curl http://127.0.0.1:7000/manager/kv/name

# 动态扩缩容
curl -X POST http://127.0.0.1:7000/manager/nodes
curl -X DELETE http://127.0.0.1:7000/manager/nodes/node-4

# 故障实验
curl -X POST http://127.0.0.1:7000/manager/nodes/node-1/stop
# 等待重选后继续写入
curl -X PUT "http://127.0.0.1:7000/manager/kv/still?value=alive"
curl -X POST http://127.0.0.1:7000/manager/nodes/node-1/start

# 测试
mvn test
mvn package "-Dcluster.e2e=true"   # 端到端测试
```

---

## 十五、推荐学习资源

- [Raft 论文](https://raft.github.io/raft.pdf) - The Raft Consensus Algorithm
- [Raft 可视化](https://raft.github.io/) - Raft 动态演示
- [Raft 动画](https://thesecretlivesofdata.com/raft/) - 交互式 Raft 学习
- [Martin Kleppmann - DDIA](https://dataintegrity.net/) - 设计数据密集型应用
- [Consul 官方文档](https://developer.hashicorp.com/consul/docs) - Consul 架构与 API
- [Consul vs ZooKeeper vs etcd](https://developer.hashicorp.com/consul/docs/architecture/consul-vs-zookeeper) - 共识系统对比
