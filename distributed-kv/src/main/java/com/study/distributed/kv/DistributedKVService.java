package com.study.distributed.kv;

import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.kv.command.KVCommand;
import com.study.distributed.kv.statemachine.KVStateMachine;
import com.study.distributed.raft.core.RaftNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

/**
 * 分布式 KV 服务 - 基于 Raft 共识
 *
 * 学习要点:
 * - 写操作: 必须走 Raft 共识 (Leader 处理)
 * - 读操作: 可以走 Leader 保证线性一致性，也可以走任意节点 (最终一致性)
 * - 如果当前节点不是 Leader，返回 Leader 信息让客户端重定向
 */
public class DistributedKVService {

    private static final Logger log = LoggerFactory.getLogger(DistributedKVService.class);
    private static final JsonSerializer SERIALIZER = new JsonSerializer();

    private final RaftNode raftNode;
    private final KVStateMachine stateMachine;

    public DistributedKVService(RaftNode raftNode, KVStateMachine stateMachine) {
        this.raftNode = raftNode;
        this.stateMachine = stateMachine;
    }

    /**
     * 写入 KV - 走 Raft 共识
     */
    public CompletableFuture<String> put(String key, String value) {
        KVCommand cmd = KVCommand.put(key, value);
        return submitCommand(cmd);
    }

    /**
     * 删除 KV - 走 Raft 共识
     */
    public CompletableFuture<String> delete(String key) {
        KVCommand cmd = KVCommand.delete(key);
        return submitCommand(cmd);
    }

    /**
     * 读取 KV - 直接读本地状态机
     * 注意: 这只保证最终一致性。要线性一致性需要走 Raft (读也提交到日志)
     */
    public String get(String key) {
        Object result = stateMachine.getKvStore().get(key);
        return result != null ? result.toString() : null;
    }

    private CompletableFuture<String> submitCommand(KVCommand cmd) {
        try {
            byte[] commandBytes = SERIALIZER.serialize(cmd);
            return raftNode.submitCommand(commandBytes)
                    .thenApply(v -> "OK");
        } catch (Exception e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    public RaftNode getRaftNode() {
        return raftNode;
    }

    public KVStateMachine getStateMachine() {
        return stateMachine;
    }
}
