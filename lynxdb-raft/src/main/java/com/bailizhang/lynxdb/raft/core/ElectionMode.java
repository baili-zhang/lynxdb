package com.bailizhang.lynxdb.raft.core;

/**
 * 配置场景：集群初始化启动时
 */
public interface ElectionMode {
    /**
     * 需要得到 leader 确认才能启动选举计时器
     */
    String FOLLOWER = "follower";
    /**
     * 选举计时器超时即可转换成 candidate
     */
    String LEADER = "leader";
}
