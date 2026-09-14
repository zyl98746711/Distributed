package com.study.distributed.raft.state;

/**
 * 状态机接口
 *
 * 学习要点:
 * Raft 共识的最终目的是将命令应用到状态机
 * 状态机是确定性的: 相同的命令序列产生相同的状态
 * 典型实现: KV 存储、数据库等
 */
public interface StateMachine {

    /**
     * 应用一条已提交的命令到状态机
     *
     * @param command 命令数据 (序列化后的字节)
     * @return 执行结果
     */
    Object apply(byte[] command);
}
