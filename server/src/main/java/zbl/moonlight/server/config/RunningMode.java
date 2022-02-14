package zbl.moonlight.server.config;

public enum RunningMode {
    /* 单节点运行 */
    SINGLE,
    /* 主从复制模式 */
    REPLICATION,
    /* 集群模式 */
    CLUSTER
}
