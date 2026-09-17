package com.study.distributed.node.config;

import com.study.distributed.kv.DistributedKVService;
import com.study.distributed.kv.statemachine.KVStateMachine;
import com.study.distributed.raft.config.RaftConfig;
import com.study.distributed.raft.core.RaftNode;
import com.study.distributed.raft.transport.SocketRaftRpcService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * 节点装配 - 每个 JVM 进程组装一个完整的 Raft 节点
 *
 * 装配顺序 (不可颠倒):
 *   1. 解析 peers -> PeerRegistry
 *   2. SocketRaftRpcService (监听 RPC 端口)
 *   3. KVStateMachine
 *   4. RaftNode (注入 rpcService, 共识算法与传输层通过接口解耦)
 *   5. rpc.bind(node) -> rpc.start() (入站端口先就绪) -> node.start() (再开始选举计时)
 *
 * 销毁顺序由 Spring 依赖图保证: kvService -> raftNode -> raftRpcService
 * (@Bean(destroyMethod = "stop") 会停掉选举线程 / 心跳线程 / 监听端口)
 */
@Configuration
@EnableConfigurationProperties(RaftNodeProperties.class)
public class NodeConfig {

    private static final Logger log = LoggerFactory.getLogger(NodeConfig.class);

    private final RaftNodeProperties props;

    public NodeConfig(RaftNodeProperties props) {
        this.props = props;
    }

    @Bean
    public PeerRegistry peerRegistry() {
        return new PeerRegistry(PeerSpec.parse(props.getPeers()));
    }

    @Bean(destroyMethod = "stop")
    public SocketRaftRpcService raftRpcService() {
        return new SocketRaftRpcService(props.getNodeId(), props.getRpcPort());
    }

    @Bean
    public KVStateMachine kvStateMachine() {
        return new KVStateMachine();
    }

    @Bean(destroyMethod = "stop")
    public RaftNode raftNode(SocketRaftRpcService rpcService, KVStateMachine stateMachine,
                             PeerRegistry registry) throws IOException {
        RaftConfig config = RaftConfig.of(
                props.getNodeId(),
                props.getHost(),
                props.getRpcPort(),
                props.getElectionTimeoutMin(),
                props.getElectionTimeoutMax(),
                props.getHeartbeatInterval());

        RaftNode node = new RaftNode(config, registry.nodes(), stateMachine, rpcService);

        // 先启动入站服务再启动节点定时器, 确保其他节点的连接能立即得到响应
        rpcService.bind(node);
        rpcService.start();
        node.start();

        log.info("[{}] 节点启动完成, RPC 端口 {}, 集群节点 {}",
                props.getNodeId(), props.getRpcPort(),
                registry.all().stream().map(PeerSpec::id).toList());
        return node;
    }

    @Bean
    public DistributedKVService kvService(RaftNode raftNode, KVStateMachine stateMachine) {
        return new DistributedKVService(raftNode, stateMachine);
    }
}
