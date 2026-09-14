package com.study.distributed.kv.command;

import java.io.Serializable;

/**
 * KV 命令 - 使用 Record 定义
 *
 * 学习要点: 所有写操作都作为 Raft 命令提交
 * 命令必须是确定性的: 相同命令序列 -> 相同结果
 */
public record KVCommand(
        CommandType type,
        String key,
        String value
) implements Serializable {

    public enum CommandType {
        PUT, GET, DELETE
    }

    public static KVCommand put(String key, String value) {
        return new KVCommand(CommandType.PUT, key, value);
    }

    public static KVCommand get(String key) {
        return new KVCommand(CommandType.GET, key, null);
    }

    public static KVCommand delete(String key) {
        return new KVCommand(CommandType.DELETE, key, null);
    }
}
