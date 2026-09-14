package com.study.distributed.raft.core;

import com.study.distributed.common.model.NodeInfo;
import com.study.distributed.raft.rpc.*;

/**
 * Raft RPC 服务接口
 *
 * 学习要点:
 * 将网络通信抽象为接口，Raft 核心算法不关心底层用什么传输
 * 可以用 Socket、HTTP、甚至内存直接调用 (测试用)
 */
public interface RaftRpcService {

    /**
     * 发送 RequestVote RPC 到指定节点
     */
    RequestVoteResponse requestVote(NodeInfo target, RequestVoteRequest request) throws Exception;

    /**
     * 发送 AppendEntries RPC 到指定节点
     */
    AppendEntriesResponse appendEntries(NodeInfo target, AppendEntriesRequest request) throws Exception;
}
