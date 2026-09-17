package com.study.distributed.raft.membership;

import com.study.distributed.common.serializer.JsonSerializer;

/** 配置日志包含完整旧、新成员，重放时不依赖本地部署配置。 */
public final class MemberCodec {
    private MemberCodec() {}
    public static byte[] encode(Membership membership) {
        try {
            return JsonSerializer.getMapper().writeValueAsBytes(membership);
        } catch (Exception e) {
            throw new IllegalArgumentException("成员配置编码失败", e);
        }
    }
    public static Membership decode(byte[] bytes) {
        try {
            return JsonSerializer.getMapper().readValue(bytes, Membership.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("成员配置解码失败", e);
        }
    }
}
