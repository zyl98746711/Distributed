package com.study.distributed.common.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * 节点工具类
 */
public final class NodeUtil {

    private NodeUtil() {}

    /**
     * 获取本机 IP
     */
    public static String getLocalIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }
}
