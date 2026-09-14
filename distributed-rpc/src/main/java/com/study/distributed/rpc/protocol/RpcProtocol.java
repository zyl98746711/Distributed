package com.study.distributed.rpc.protocol;

import com.study.distributed.common.serializer.Serializer;
import com.study.distributed.common.serializer.JsonSerializer;

import java.io.*;

/**
 * 自定义 RPC 协议编解码器
 *
 * <h3>为什么需要自定义协议?</h3>
 * <p>TCP 是流式协议，没有消息边界。如果连续发送两条消息:</p>
 * <pre>
 *   发送: "Hello" + "World"
 *   接收端可能读到: "HelloWorld" 或 "Hel" + "loWorld" 或 "HelloWor" + "ld"
 * </pre>
 * <p>这就是 TCP 粘包/拆包问题。解决方案就是自定义协议，让接收端知道:</p>
 * <ul>
 *   <li>哪里是消息的开始 (魔数)</li>
 *   <li>消息头有多长 (固定头部)</li>
 *   <li>消息体有多长 (数据长度字段)</li>
 * </ul>
 *
 * <h3>协议格式</h3>
 * <pre>
 * +--------+--------+--------+--------+
 * | 魔数   | 版本   | 类型   | 序列化 |  固定头部 (4 字节)
 * | 0xAB   |  0x01  | 1=请求 |  方式  |
 * +--------+--------+--------+--------+
 * |    请求ID长度 (4字节, 大端)       |  告诉接收端 ID 有多长
 * +--------------------------------+
 * |    数据体长度 (4字节, 大端)       |  告诉接收端数据有多长
 * +--------------------------------+
 * |    请求ID (变长)                  |  如 "req-1"
 * +--------------------------------+
 * |    数据体 (变长)                  |  序列化后的请求/响应
 * +--------------------------------+
 * </pre>
 *
 * <h3>各字段作用</h3>
 * <ul>
 *   <li><b>魔数 (0xAB)</b>: 快速识别协议类型，过滤非法连接</li>
 *   <li><b>版本</b>: 协议升级时兼容旧版本</li>
 *   <li><b>类型</b>: 区分请求和响应</li>
 *   <li><b>序列化方式</b>: 支持多种序列化策略 (JSON/JDK/...) </li>
 *   <li><b>请求ID</b>: 同一连接上多个请求并行时，用于匹配响应</li>
 *   <li><b>数据长度</b>: 解决 TCP 粘包，告诉接收端读多少字节</li>
 * </ul>
 */
public class RpcProtocol {

    /** 魔数: 标识这是一个 RPC 协议包 */
    public static final byte MAGIC = (byte) 0xAB;
    /** 协议版本 */
    public static final byte VERSION = 1;
    /** 消息类型: 1=请求, 2=响应 */
    public static final byte TYPE_REQUEST = 1;
    public static final byte TYPE_RESPONSE = 2;

    /** 头部固定长度: magic(1) + version(1) + type(1) + serializer(1) + idLen(4) + dataLen(4) = 12 */
    public static final int HEADER_LENGTH = 12;

    /**
     * 编码请求消息
     */
    public static byte[] encodeRequest(RpcRequest request, Serializer serializer) throws Exception {
        byte[] idBytes = request.requestId().getBytes();
        byte[] data = serializer.serialize(request);
        return buildFrame(TYPE_REQUEST, serializer.getTypeId(), idBytes, data);
    }

    /**
     * 编码响应消息
     */
    public static byte[] encodeResponse(RpcResponse response, Serializer serializer) throws Exception {
        byte[] idBytes = response.requestId().getBytes();
        byte[] data = serializer.serialize(response);
        return buildFrame(TYPE_RESPONSE, serializer.getTypeId(), idBytes, data);
    }

    private static byte[] buildFrame(byte type, byte serializerId, byte[] idBytes, byte[] data) {
        byte[] frame = new byte[HEADER_LENGTH + idBytes.length + data.length];
        frame[0] = MAGIC;
        frame[1] = VERSION;
        frame[2] = type;
        frame[3] = serializerId;
        // ID 长度 (4字节, 大端)
        frame[4] = (byte) (idBytes.length >> 24);
        frame[5] = (byte) (idBytes.length >> 16);
        frame[6] = (byte) (idBytes.length >> 8);
        frame[7] = (byte) idBytes.length;
        // 数据长度 (4字节, 大端)
        frame[8] = (byte) (data.length >> 24);
        frame[9] = (byte) (data.length >> 16);
        frame[10] = (byte) (data.length >> 8);
        frame[11] = (byte) data.length;
        // ID 和数据
        System.arraycopy(idBytes, 0, frame, HEADER_LENGTH, idBytes.length);
        System.arraycopy(data, 0, frame, HEADER_LENGTH + idBytes.length, data.length);
        return frame;
    }

    /**
     * 从输入流解码一帧数据
     */
    public static Frame decode(InputStream in) throws IOException {
        // 读取头部
        byte[] header = in.readNBytes(HEADER_LENGTH);
        if (header.length < HEADER_LENGTH) {
            throw new IOException("连接关闭，无法读取完整头部");
        }
        if (header[0] != MAGIC) {
            throw new IOException("非法魔数: " + header[0]);
        }

        byte type = header[2];
        byte serializerId = header[3];
        int idLen = readInt(header, 4);
        int dataLen = readInt(header, 8);

        // 读取 ID
        byte[] idBytes = in.readNBytes(idLen);
        if (idBytes.length < idLen) throw new IOException("连接关闭，无法读取完整ID");
        String requestId = new String(idBytes);

        // 读取数据体
        byte[] data = in.readNBytes(dataLen);
        if (data.length < dataLen) throw new IOException("连接关闭，无法读取完整数据");

        return new Frame(type, serializerId, requestId, data);
    }

    private static int readInt(byte[] buf, int offset) {
        return ((buf[offset] & 0xFF) << 24)
             | ((buf[offset + 1] & 0xFF) << 16)
             | ((buf[offset + 2] & 0xFF) << 8)
             | (buf[offset + 3] & 0xFF);
    }

    /**
     * 解码后的帧
     */
    public record Frame(byte type, byte serializerId, String requestId, byte[] data) {}
}
