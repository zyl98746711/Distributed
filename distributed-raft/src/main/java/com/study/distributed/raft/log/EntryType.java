package com.study.distributed.raft.log;

/** 普通命令与联合共识的两阶段配置日志。 */
public enum EntryType {
    COMMAND, CONFIG_JOINT, CONFIG_FINAL
}
