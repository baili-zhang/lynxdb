package zbl.moonlight.raft.state;

import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.socket.client.ServerNode;

import java.nio.channels.SelectionKey;
import java.util.List;

/**
 * Raft 定义的状态机的接口
 */
public interface StateMachine {

    /**
     * 获取集群的所有节点
     * @return 集群的所有节点
     */
    List<ServerNode> clusterNodes();

    /**
     * 获取当前节点
     * @return 当前节点
     */
    ServerNode currentNode();

    /**
     * 应用日志条目
     * @param entries 日志条目
     */
    void apply(Entry[] entries);

    /**
     * 执行客户端命令请求
     * @param command 命令
     */
    void exec(SelectionKey key, byte[] command);
}
