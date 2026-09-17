# Distributed Study Framework

> 基于 JDK 21 + Spring Boot 3 的分布式系统手写学习框架
>
> **核心理念**: 不依赖任何外部中间件 (Redis/ZooKeeper/Nacos)，纯 Java 手写实现分布式系统核心组件，从原理出发深入理解分布式。

---

## 目录

- [项目结构](#项目结构)
- [学习路线图](#学习路线图)
- [模块详解](#模块详解)
  - [1. distributed-common - 公共基础](#1-distributed-common---公共基础)
  - [2. distributed-rpc - 手写 RPC 框架](#2-distributed-rpc---手写-rpc-框架)
  - [3. distributed-raft - Raft 共识算法](#3-distributed-raft---raft-共识算法)
  - [4. distributed-kv - 分布式 KV 存储](#4-distributed-kv---分布式-kv-存储)
  - [5. distributed-lock - 分布式锁](#5-distributed-lock---分布式锁)
  - [6. distributed-id - 分布式 ID](#6-distributed-id---分布式-id)
  - [7. distributed-ratelimit - 分布式限流](#7-distributed-ratelimit---分布式限流)
  - [8. distributed-demo - 演示应用](#8-distributed-demo---演示应用)
  - [9. distributed-node - 独立节点进程](#9-distributed-node---独立节点进程)
  - [10. distributed-manager - 节点管理服务](#10-distributed-manager---节点管理服务)
- [快速开始](#快速开始)
- [核心知识点索引](#核心知识点索引)

---

## 项目结构

```
Distributed/
├── pom.xml                          # 父 POM (Maven 聚合工程)
│
├── distributed-common/              # [基础] 公共模块
│   └── model/exception/serializer/util
│
├── distributed-rpc/                 # [通信] 手写 RPC 框架
│   ├── protocol/                    #   自定义二进制协议
│   ├── transport/                   #   Socket + Virtual Threads 传输层
│   ├── registry/                    #   服务注册与发现
│   └── proxy/                       #   JDK 动态代理
│
├── distributed-raft/                # [共识] Raft 算法实现
│   ├── core/                        #   RaftNode 核心状态机
│   ├── log/                         #   日志存储 (命令 + 两阶段配置日志)
│   ├── membership/                  #   动态成员与双多数派
│   ├── transport/                   #   真实 Socket Raft RPC
│   ├── rpc/                         #   RequestVote / AppendEntries RPC
│   ├── state/                       #   状态机接口
│   └── config/                      #   配置
│
├── distributed-kv/                  # [存储] 分布式 KV 存储
│   ├── command/                     #   命令模型
│   ├── store/                       #   存储引擎
│   ├── statemachine/                #   Raft 状态机实现
│   └── controller/                  #   REST API
│
├── distributed-lock/                # [锁] 分布式锁
│   ├── api/                         #   接口定义
│   ├── impl/                        #   Simple + Raft 两种实现
│   ├── annotation/                  #   @DLock 注解
│   └── aop/                         #   AOP 切面
│
├── distributed-id/                  # [ID] 分布式 ID 生成器
│   ├── api/                         #   接口定义
│   ├── snowflake/                   #   雪花算法
│   └── segment/                     #   号段模式
│
├── distributed-ratelimit/           # [限流] 分布式限流
│   ├── api/                         #   接口定义
│   ├── algorithm/                   #   四种算法实现
│   ├── annotation/                  #   @RateLimit 注解
│   └── aop/                         #   AOP 切面
│
├── distributed-node/                # [节点] 独立节点进程 (每进程一个 Raft 节点)
│   ├── config/                      #   节点装配 (SocketRaftRpcService + RaftNode)
│   └── controller/                  #   节点 HTTP 接口 (KV + Leader 转发 + 状态观测)
│
├── distributed-manager/             # [管理] 配置中心 + 本机进程编排 + 统一网关
│   ├── cluster/                     #   进程生命周期与 Leader 发现
│   ├── client/                      #   节点 HTTP 通信
│   └── controller/                  #   动态扩缩容 /manager/nodes 与 /manager/kv
│
└── distributed-demo/                # [演示] 整合所有模块 (单 JVM 3 节点)
    ├── config/                      #   Spring 配置
    └── controller/                  #   演示 API
```

## 学习路线图

建议按照以下顺序学习，每个模块都依赖前面的模块：

```
  ① common ──→ ② rpc ──→ ③ raft ──→ ④ kv ──→ ⑤ lock
                                │
                                ├──────────→ ⑥ id (独立)
                                │
                                └──────────→ ⑦ ratelimit (独立)
```

| 阶段 | 模块 | 学习目标 | 预计时间 |
|------|------|---------|---------|
| 1 | common | 了解项目基础模型、序列化设计 | 30min |
| 2 | rpc | **理解 RPC 原理** (协议、传输、代理) | 2h |
| 3 | raft | **深入共识算法** (选举、复制、安全) | 4h |
| 4 | kv | 理解状态机 + Raft 应用 | 1h |
| 5 | lock | 理解分布式锁的核心问题 | 1.5h |
| 6 | id | 理解分布式 ID 的挑战 | 1h |
| 7 | ratelimit | 掌握四种限流算法 | 1h |

---

## 模块详解

### 1. distributed-common - 公共基础

**文件清单**:
| 文件 | 作用 |
|------|------|
| `Result<T>` | 统一响应封装 (JDK 21 Record) |
| `NodeInfo` | 节点信息模型 |
| `Serializer` | 序列化器接口 (可插拔策略) |
| `JsonSerializer` | JSON 序列化实现 (基于 Jackson) |
| `JdkSerializer` | JDK 原生序列化 (对比参考) |
| `RpcException` / `DistributedLockException` / `RateLimitException` | 异常体系 |

**学习要点**: 理解为什么序列化器需要接口抽象 —— 不同场景需要不同的序列化策略 (JSON 可读性好适合调试，二进制性能好适合生产)。

---

### 2. distributed-rpc - 手写 RPC 框架

**这是后续所有模块的通信基础，必须首先理解。**

#### 核心流程

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
     │                                      │
```

#### 自定义协议格式

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

**学习要点**:
- **为什么需要自定义协议?** TCP 是流式协议，需要解决粘包/拆包问题
- **魔数的作用?** 快速识别协议类型，过滤非法连接
- **Virtual Threads 的优势?** 每个连接一个虚拟线程，阻塞等待不浪费平台线程

#### 关键文件

| 文件 | 核心职责 |
|------|---------|
| `RpcProtocol` | 协议编解码，理解帧结构 |
| `RpcServer` | 服务端：接受连接 → 解码请求 → 反射调用 → 编码响应 |
| `RpcClient` | 客户端：编码请求 → 发送 → 等待响应 (requestId 匹配) |
| `RpcProxyFactory` | JDK 动态代理，让远程调用像本地调用 |
| `ServiceRegistry` | 服务注册表接口，理解服务发现原理 |

---

### 3. distributed-raft - Raft 共识算法

**这是整个项目最核心的模块，建议配合 [Raft 论文](https://raft.github.io/raft.pdf) 和 [可视化演示](https://raft.github.io/) 一起学习。**

#### 三种角色状态

```
         选举超时                    获得多数票
FOLLOWER ────────→ CANDIDATE ────────────────→ LEADER
   ↑                  │                           │
   │                  │ 发现更高任期                │ 发现更高任期
   │                  ↓                           │
   └──────────────────────────────────────────────┘
```

#### Leader 选举流程

```
1. Follower 在 electionTimeout 内没收到心跳
2. 变为 Candidate, currentTerm++
3. 投票给自己, 向所有节点发送 RequestVote RPC
4. 收到多数票 → 成为 Leader
5. 立即发送 AppendEntries 心跳 (确立权威)
```

#### 日志复制流程

```
Client 请求 → Leader 追加日志到本地
           → Leader 发送 AppendEntries 给所有 Follower
           → 多数派确认后 → commitIndex 推进
           → 应用到状态机 (StateMachine.apply)
```

#### 核心数据结构

```java
// 每个节点持久化的状态
long currentTerm;        // 当前任期
String votedFor;         // 当前任期投给了谁
LogStore log;            // 日志存储

// 每个节点易失的状态
NodeRole role;           // FOLLOWER / CANDIDATE / LEADER
long commitIndex;        // 已提交的日志索引
long lastApplied;        // 已应用到状态机的索引

// Leader 独有的状态
Map<String, Long> nextIndex;   // 每个 Follower 下一条要发的日志索引
Map<String, Long> matchIndex;  // 每个 Follower 已匹配的最高日志索引
```

#### 关键文件

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

---

### 4. distributed-kv - 分布式 KV 存储

**用 Raft 共识构建一个真正可用的分布式存储，验证 Raft 的正确性。**

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

**学习要点**: 写操作必须走 Raft (保证一致性)，读操作可以直接读本地状态机 (最终一致性)。

---

### 5. distributed-lock - 分布式锁

**两种实现对比学习:**

| 特性 | SimpleDistributedLock | RaftDistributedLock |
|------|----------------------|---------------------|
| 实现方式 | 内存 ConcurrentHashMap + TTL | 基于 Raft KV 共识 |
| 跨节点 | 不支持 | 支持 |
| 容错 | 单点故障 | Raft 保证高可用 |
| 可重入 | 支持 | 支持 |
| 看门狗 | 无 | 支持自动续期 |
| 适用场景 | 单机学习 | 生产级分布式锁 |

**学习要点**: 分布式锁的核心问题 —— 互斥、死锁、容错、重入。看门狗机制防止业务未完成锁就过期。

---

### 6. distributed-id - 分布式 ID

#### 雪花算法 (Snowflake)

```
 0 | 41位时间戳 | 10位机器ID | 12位序列号
   | (69年)     | (1024节点) | (4096/毫秒/节点)
```

**关键问题**: 时钟回拨怎么办? 代码中实现了等待策略。

#### 号段模式

```
外部存储 (DB/文件) ──批量获取──→ [号段 Buffer] ──逐个分配──→ ID
                                  ↓ 用到20%时
                                异步预加载下一个号段
```

---

### 7. distributed-ratelimit - 分布式限流

**四种算法对比:**

| 算法 | 原理 | 优点 | 缺点 |
|------|------|------|------|
| **固定窗口** | 时间分窗口计数 | 简单 | 边界 2x 流量 |
| **滑动窗口** | 记录每个请求时间戳 | 精确 | 内存占用大 |
| **令牌桶** | 固定速率放令牌，请求取令牌 | 允许突发 | - |
| **漏桶** | 固定速率处理请求 | 恒定速率 | 无法突发 |

```
令牌桶: 允许突发              漏桶: 恒定速率
  ↓ ↓↓ ↓    ↓↓               ↓ ↓ ↓ ↓ ↓ ↓ ↓
  ╔═══════╗                   ╔═══════╗
  ║ Token ║ → 有就用          ║ Water ║ → 固定流出
  ║ Bucket║                   ║ Bucket║
  ╚═══════╝                   ╚═══════╝
```

---

### 8. distributed-demo - 演示应用

启动后在同一 JVM 内运行 3 个 Raft 节点组成集群，自动选举 Leader。

```
┌─────────────────────────────────────────────────────┐
│                    JVM 进程                          │
│                                                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │  node-1  │  │  node-2  │  │  node-3  │         │
│  │ RaftNode │  │ RaftNode │  │ RaftNode │         │
│  │ KV SM    │  │ KV SM    │  │ KV SM    │         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
│       └──────────────┼──────────────┘               │
│              InMemoryRaftRpcService                 │
│                                                     │
│  HTTP API (:8080) → 路由到指定节点                  │
└─────────────────────────────────────────────────────┘
```

可体验的 API:

```bash
# 集群状态 (查看谁当选 Leader)
GET /cluster/status

# KV 写入 (指定节点)
PUT /kv/node-1/name?value=Distributed

# KV 写入 (自动路由到 Leader)
PUT /kv/leader/name?value=Distributed

# KV 读取 (从指定节点读)
GET /kv/node-2/name

# 查看所有节点数据 (对比一致性)
GET /kv/all/data

# ID 生成
GET /demo/id               # 生成一个 ID (含解析)
GET /demo/id/batch         # 批量生成 10000 个 (性能测试)

# 限流对比
GET /demo/ratelimit/compare # 四种算法对比
```

---

### 9. distributed-node - 独立节点进程

**把每个 Raft 节点放进独立 JVM 进程, 通过真实 TCP 通信 —— 这才是真正的"分布式"。**

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│  进程 node-1  │     │  进程 node-2  │     │  进程 node-3  │
│  ┌─────────┐ │     │  ┌─────────┐ │     │  ┌─────────┐ │
│  │ RaftNode│ │     │  │ RaftNode│ │     │  │ RaftNode│ │
│  │ KV 状态机 │ │     │  │ KV 状态机 │ │     │  │ KV 状态机 │ │
│  │ Socket  │ │     │  │ Socket  │ │     │  │ Socket  │ │
│  │ RPC     │ │     │  │ RPC     │ │     │  │ RPC     │ │
│  └────┬────┘ │     │  └────┬────┘ │     │  └────┬────┘ │
│  RPC :9001   │     │  RPC :9002   │     │  RPC :9003   │
│  HTTP :8001  │     │  HTTP :8002  │     │  HTTP :8003  │
└───────┼──────┘     └───────┼──────┘     └───────┼──────┘
        └──────────────── TCP 真实网络 ────────────────┘
```

与 demo 模式 (单 JVM) 的对比:

| 维度 | distributed-demo | distributed-node |
|------|------------------|------------------|
| 节点通信 | `InMemoryRaftRpcService` 直接方法调用 | `SocketRaftRpcService` 真实 TCP |
| 进程隔离 | 无 (一个进程崩全崩) | 每节点独立 JVM, 可单独 kill |
| 故障实验 | 无法模拟机器宕机 | kill / 重启节点, 观察重选与日志追赶 |
| 客户端入口 | 集中式 /kv/{nodeId}/... | 推荐统一经 manager 的 /manager/kv 访问 |

**关键文件**:

| 文件 | 核心职责 |
|------|---------|
| `RaftProtocol` | 长度前缀帧 + 消息类型标签, 解决粘包 |
| `SocketRaftRpcService` | 长连接 + requestId 匹配响应 + 超时自动重连 |
| `NodeConfig` | 每进程装配 SocketRaftRpcService + RaftNode |
| `NodeKVController` | 写请求自动转发到 Leader (`X-Raft-Forwarded` 防循环) |
| `NodeController` | /node/status、/node/log 状态观测 |
| `NodeMembershipController` | /raft/members 查询与成员变更 (非 Leader 返回 409) |

---

### 10. distributed-manager - 节点管理服务

节点不再使用 node-1/2/3 profile；初始部署清单集中到 manager 的 `application.yml`。
manager 用 `ProcessBuilder` 为每个节点注入独立 ID、HTTP/RPC 端口及 bootstrap 清单。

```text
其他服务 ── HTTP :7000 ──> manager（控制平面，不参与投票）
                          ├─ 配置清单 / 本机进程启停
                          └─ /manager/kv 自动发现并路由 Leader
                                      │
                     node-1 <── Raft TCP ──> node-2 <──> node-N
                                数据平面 / 联合共识
```

| 组件 | 职责 |
|------|------|
| `ManagerProperties` | 节点 jar 路径、预置清单、auto-start |
| `ManagedNode` / `NodeProcessManager` | 分离部署与成员状态，启动、探活、通过启动标识及进程创建时间核实 PID、停止进程 |
| `LeaderLocator` | 每 2 秒探测 Leader；缓存失效时重新发现 |
| `NodeHttpClient` | 有限连接/读取超时，严格编码 URI，防止无限等待 |
| `ManagerController` | 集群状态、串行扩缩容、进程控制、KV 网关 |

**Joint Consensus**：先追加 `CONFIG_JOINT(C_old,C_new)`，再在它提交后追加 `CONFIG_FINAL(C_new)`。
联合期选举和日志提交要求旧、新配置各自的多数派；FINAL 追加后切换为新配置多数派。
配置追加即用于选举，日志冲突截断后重建配置；新 Leader 的 no-op 日志用于推进旧任期日志与未完成变更。
新增节点先以不包含自身的 bootstrap 启动，收到联合配置前不投票/参选；移除成员必须提交 FINAL 后才停止进程。

**边界**：
- 本地教学实现，仅支持 `127.0.0.1`/`localhost` 进程编排；manager 默认仅监听回环地址，没有鉴权，不能直接暴露公网。
- 一次变更一个成员；拒绝并发变更及移除最后一个成员。4 节点需要 3 票，容错能力并不高于 3 节点。
- 部署清单保存在 `logs/manager-nodes.json`，重启优先读取它；已移除 ID/端口保留，避免误复用。共识成员以 Leader 已提交配置为准。
- 超时不代表变更失败，保留 PENDING 部署记录；后续扩缩容先向 Leader 对账。只对已确认成功但未追平的 ADD 尝试补偿 REMOVE。
- manager 退出不自动停止节点，可重启接管；停机前可逐个调用 stop 接口。stop/start 只控制进程，不改变成员数。
- manager 是单实例入口，不实现高可用或跨机 agent；节点 Raft 通信不经过 manager。
- Raft 日志、任期、投票及 KV 仍在内存中。单节点重启可从存活多数派追赶，但不保证掉电安全；全体重启丢失 KV，不能作为生产存储。
- 网关读路由至 Leader，但没有 ReadIndex/租约验证，**不是线性一致读**。网络超时的写入结果可能未知，不会盲目自动重试。

---

## 快速开始

### 环境要求

- JDK 21+
- Maven 3.8+

### 编译

```bash
mvn clean compile
```

### 启动演示

```bash
cd distributed-demo
mvn spring-boot:run
```

### 独立进程集群模式 (真实分布式)

从项目根目录执行以下命令 (Windows/Linux/macOS 通用)，manager 自动启动 3 个独立 JVM。
默认 manager HTTP 7000，节点 HTTP 8001-8003，Raft TCP 9001-9003；原启动/停止脚本已由 manager 取代。

```bash
mvn clean package
# 若 Windows 的 target 被占用，可改用 mvn package
java -jar distributed-manager/target/distributed-manager-1.0.0-SNAPSHOT.jar
```

首次迁移时先停止旧版本节点，避免端口冲突。新 manager 不会接管无法核实身份的旧脚本进程。
非项目根目录启动时使用 `--manager.node-jar=<绝对路径>`，日志和部署状态路径也可通过 manager 配置覆盖。
`--manager.auto-start=false` 禁止自动拉起节点。节点日志追加写入 `logs/node-N.log`。

集群就绪后，其他服务只需知道 `http://127.0.0.1:7000`：

```bash
# 状态与统一读写 (Windows PowerShell 可使用 curl.exe)
curl http://127.0.0.1:7000/manager/cluster
curl -X PUT "http://127.0.0.1:7000/manager/kv/name?value=raft"
curl http://127.0.0.1:7000/manager/kv/name
curl http://127.0.0.1:7000/manager/kv/all

# 动态扩容：自动分配 node-N / HTTP / RPC 端口，也可传 JSON 指定 host/httpPort/rpcPort
curl -X POST http://127.0.0.1:7000/manager/nodes
# 新建集群首次扩容通常分配 node-4；以返回的 id 为准
curl http://127.0.0.1:8004/raft/members
curl -X DELETE http://127.0.0.1:7000/manager/nodes/node-4

# 故障实验：将 node-1 替换为 /manager/cluster 显示的 Leader
curl -X POST http://127.0.0.1:7000/manager/nodes/node-1/stop
# 等待数秒重选后，网关无需改地址即可继续写入
curl -X PUT "http://127.0.0.1:7000/manager/kv/still?value=alive"
curl -X POST http://127.0.0.1:7000/manager/nodes/node-1/start

# 节点接口保留用于观察配置日志与复制进度
curl http://127.0.0.1:8001/node/log
curl http://127.0.0.1:8001/raft/members
```

### 测试

```bash
# 单元测试：多数派、日志冲突、动态成员、Leader 退役及配置变更中断恢复
mvn test
# 真实进程端到端测试：先打包 node，再启动独立端口的测试集群，结束自动停止
mvn package "-Dcluster.e2e=true"
```

端到端测试覆盖统一网关、中文/特殊字符、三节点复制、动态扩缩容、重复启动保护、
manager 重启接管以及 Leader 故障后的重选与日志追赶；日志位于 `distributed-manager/target/e2e/`。
以下接口属于原有 demo 模式：

```bash
# 查看集群状态 (观察 Leader 选举结果)
curl http://localhost:8080/cluster/status

# 写入 KV (自动路由到 Leader)
curl -X PUT "http://localhost:8080/kv/leader/name?value=Distributed"

# 从各节点读取 (验证数据一致性)
curl http://localhost:8080/kv/node-1/name
curl http://localhost:8080/kv/node-2/name
curl http://localhost:8080/kv/node-3/name

# 查看所有节点数据对比
curl http://localhost:8080/kv/all/data

# 生成 ID
curl http://localhost:8080/demo/id

# 限流对比
curl http://localhost:8080/demo/ratelimit/compare
```

---

## 核心知识点索引

| 知识点 | 所在模块 | 关键文件 |
|--------|---------|---------|
| TCP 粘包/拆包 | rpc | `RpcProtocol` |
| 长度前缀帧 (粘包解决) | raft | `RaftProtocol` |
| 自定义二进制协议 | rpc | `RpcProtocol` |
| JDK 动态代理 | rpc | `RpcProxyFactory` |
| Virtual Threads | rpc, raft | `RpcServer`, `RpcClient` |
| Leader 选举 | raft | `RaftNode.startElection()` |
| 日志复制 | raft | `RaftNode.submitCommand()` |
| 日志匹配 (安全性) | raft | `RaftNode.handleAppendEntries()` |
| 投票限制 (安全性) | raft | `RaftNode.handleRequestVote()` |
| 选举超时随机化 | raft | `RaftNode.randomElectionTimeout()` |
| 心跳捎带日志 (落后节点追赶) | raft | `RaftNode.replicateTo()` |
| Joint Consensus 与双多数派 | raft | `Membership`, `RaftNode.changeMembership()` |
| 控制平面与数据平面 | manager, node | `ManagerController`, `NodeMembershipController` |
| 本机进程编排与身份校验 | manager | `NodeProcessManager` |
| Leader 发现与统一网关 | manager | `LeaderLocator`, `NodeHttpClient` |
| 长连接 + requestId 匹配响应 | raft | `SocketRaftRpcService` |
| 客户端可连任意节点 (写转发 Leader) | node | `NodeKVController` |
| kill Leader 故障实验 | node | `distributed-node` |
| 状态机确定性 | kv | `KVStateMachine` |
| 锁的 TTL + 看门狗 | lock | `RaftDistributedLock` |
| 可重入锁 | lock | `SimpleDistributedLock` |
| 雪花算法位运算 | id | `SnowflakeIdGenerator` |
| 时钟回拨处理 | id | `SnowflakeIdGenerator.nextId()` |
| 号段双 Buffer | id | `SegmentIdGenerator` |
| 固定窗口边界问题 | ratelimit | `FixedWindowLimiter` |
| 令牌桶 vs 漏桶 | ratelimit | `TokenBucketLimiter` vs `LeakyBucketLimiter` |
| Record 类型 (JDK 16+) | 全局 | 所有消息/模型类 |

---

## 技术选型说明

| 选择 | 原因 |
|------|------|
| JDK 21 | Virtual Threads 降低网络编程复杂度，Record 简化消息定义 |
| Spring Boot 3 | 提供 HTTP 管理接口、AutoConfiguration、Actuator |
| 纯手写 | 不依赖 Redis/ZK/Nacos，从原理出发理解本质 |
| JSON 序列化 | 可读性好，方便调试学习 (接口可插拔) |
| Socket + VT | 替代 Netty，降低学习门槛 |

---

## 推荐学习资源

- [Raft 论文](https://raft.github.io/raft.pdf) - The Raft Consensus Algorithm
- [Raft 可视化](https://raft.github.io/) - Raft 动态演示
- [Raft 动画](https://thesecretlivesofdata.com/raft/) - 交互式 Raft 学习
- [Martin Kleppmann - DDIA](https://dataintegrity.net/) - 设计数据密集型应用
