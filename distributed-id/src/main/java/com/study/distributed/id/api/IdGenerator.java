package com.study.distributed.id.api;

/**
 * ID 生成器接口
 *
 * 学习要点:
 * 分布式 ID 需要满足:
 * 1. 全局唯一
 * 2. 趋势递增 (有利于数据库索引)
 * 3. 高性能 (不依赖外部服务)
 * 4. 高可用
 */
public interface IdGenerator {

    /**
     * 生成下一个 ID
     */
    long nextId();
}
