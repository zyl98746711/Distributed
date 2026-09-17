package com.study.distributed.raft.transport;

import com.study.distributed.common.serializer.JsonSerializer;

import java.io.IOException;
import java.io.InputStream;

/**
 * Raft 节点间通信协议 - 长度前缀帧 + JSON
 *
 * <h3>为什么需要帧协议?</h3>
 * <p>TCP 是流式协议,没有消息边界。发送端连续写入两条消息:</p>
 * <pre>
 *   发送: "msgA" + "msgB"
 *   接收端可能读到: "msgAmsgB" 或 "msg" + "AmsgB" 或 "msgAmsg" + "B"
 * </pre>
 * <p>这就是 TCP 粘包/拆包问题。解决办法:在消息体前加一个"长度字段",
 * 接收端先读长度,再精确读取这么多字节,就能还原出每一条完整消息。</p>
 *
 * <h3>协议格式</h3>
 * <pre>
 * +------------------------------------+
 * |  消息长度 (4 字节, 大端)            |  告诉接收端 JSON 数据体有多少字节
 * +------------------------------------+
 * |  JSON 数据体 (变长)                 |
 * |  RaftMessage {                     |
 * |     type:      消息类型             |  1=VOTE_REQ  2=VOTE_RESP
 * |     requestId: 请求 ID              |  3=APPEND_REQ 4=APPEND_RESP
 * |     payload:   具体消息 record      |  如 RequestVoteRequest / AppendEntriesResponse
 * |  }                                 |
 * +------------------------------------+
 * </pre>
 *
 * <h3>与 distributed-rpc 的 RpcProtocol 对比</h3>
 * <p>RpcProtocol 是一个通用 RPC 协议:携带 serviceName/methodName/paramTypes,
 * 服务端通过反射调用任意方法,适合"通用远程调用"场景。</p>
 * <p>而 Raft 的 RPC 只有固定的两种消息 (RequestVote / AppendEntries),
 * 是强类型的,不需要反射调用。因此这里直接用"类型标签 + 强类型 JSON"的
 * 轻量帧协议,消息体直达业务 record,避免反射与参数转换的开销。</p>
 */
public class RaftProtocol {

    /** 消息类型: RequestVote 请求 (Candidate -> 其他节点) */
    public static final int TYPE_VOTE_REQ = 1;
    /** 消息类型: RequestVote 响应 */
    public static final int TYPE_VOTE_RESP = 2;
    /** 消息类型: AppendEntries 请求 (Leader -> Follower, 心跳或日志复制) */
    public static final int TYPE_APPEND_REQ = 3;
    /** 消息类型: AppendEntries 响应 */
    public static final int TYPE_APPEND_RESP = 4;

    /** 长度字段占 4 字节 (大端) */
    public static final int LENGTH_FIELD = 4;

    private RaftProtocol() {
        // 工具类,不允许实例化
    }

    /**
     * 编码一条消息: [4 字节长度][JSON 数据体]
     */
    public static byte[] encode(int type, String requestId, Object payload) throws Exception {
        byte[] body = JsonSerializer.getMapper().writeValueAsBytes(new RaftMessage(type, requestId, payload));

        byte[] frame = new byte[LENGTH_FIELD + body.length];
        // 长度字段 (大端)
        frame[0] = (byte) (body.length >> 24);
        frame[1] = (byte) (body.length >> 16);
        frame[2] = (byte) (body.length >> 8);
        frame[3] = (byte) body.length;
        // JSON 数据体
        System.arraycopy(body, 0, frame, LENGTH_FIELD, body.length);
        return frame;
    }

    /**
     * 从输入流解码一条消息 (阻塞直到读满一帧或连接关闭)
     */
    public static RaftMessage decode(InputStream in) throws IOException {
        // 先读 4 字节长度
        byte[] lengthBytes = in.readNBytes(LENGTH_FIELD);
        if (lengthBytes.length < LENGTH_FIELD) {
            throw new IOException("连接关闭, 无法读取帧长度");
        }
        int length = ((lengthBytes[0] & 0xFF) << 24)
                | ((lengthBytes[1] & 0xFF) << 16)
                | ((lengthBytes[2] & 0xFF) << 8)
                | (lengthBytes[3] & 0xFF);

        // 再按长度精确读取数据体
        byte[] body = in.readNBytes(length);
        if (body.length < length) {
            throw new IOException("连接关闭, 无法读取完整数据体");
        }

        try {
            return JsonSerializer.getMapper().readValue(body, RaftMessage.class);
        } catch (Exception e) {
            throw new IOException("Raft 消息反序列化失败: " + e.getMessage(), e);
        }
    }

    /**
     * 将 payload 转换为具体消息类型 (payload 反序列化后是 Map,这里按目标 record 转换)
     */
    public static <T> T payloadAs(RaftMessage message, Class<T> clazz) {
        return JsonSerializer.getMapper().convertValue(message.payload(), clazz);
    }

    /**
     * 解码后的 Raft 消息
     *
     * @param type      消息类型 (TYPE_VOTE_REQ / TYPE_VOTE_RESP / TYPE_APPEND_REQ / TYPE_APPEND_RESP)
     * @param requestId 请求 ID, 用于在同一个连接上匹配请求与响应
     * @param payload   具体消息体 (RequestVoteRequest / AppendEntriesResponse 等)
     */
    public record RaftMessage(int type, String requestId, Object payload) {
    }
}
