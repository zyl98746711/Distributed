package com.study.distributed.kv.statemachine;

import com.study.distributed.common.serializer.JsonSerializer;
import com.study.distributed.kv.command.KVCommand;
import com.study.distributed.kv.store.KVStore;
import com.study.distributed.kv.store.MemoryKVStore;
import com.study.distributed.raft.state.StateMachine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KV 状态机 - 将 Raft 已提交的日志应用到 KV 存储
 *
 * 学习要点:
 * 状态机是确定性的: 相同的命令序列一定产生相同的状态
 * 这是 Raft 保证所有节点状态一致的关键
 */
public class KVStateMachine implements StateMachine {

    private static final Logger log = LoggerFactory.getLogger(KVStateMachine.class);
    private static final JsonSerializer SERIALIZER = new JsonSerializer();

    private final KVStore kvStore;

    public KVStateMachine() {
        this.kvStore = new MemoryKVStore();
    }

    public KVStateMachine(KVStore kvStore) {
        this.kvStore = kvStore;
    }

    @Override
    public Object apply(byte[] command) {
        try {
            KVCommand cmd = SERIALIZER.deserialize(command, KVCommand.class);
            log.debug("应用命令: {}", cmd);

            return switch (cmd.type()) {
                case PUT -> {
                    kvStore.put(cmd.key(), cmd.value());
                    yield "OK";
                }
                case GET -> kvStore.get(cmd.key());
                case DELETE -> {
                    String old = kvStore.delete(cmd.key());
                    yield old != null ? "OK" : "NOT_FOUND";
                }
            };
        } catch (Exception e) {
            log.error("应用命令失败: {}", e.getMessage());
            return "ERROR: " + e.getMessage();
        }
    }

    public KVStore getKvStore() {
        return kvStore;
    }
}
