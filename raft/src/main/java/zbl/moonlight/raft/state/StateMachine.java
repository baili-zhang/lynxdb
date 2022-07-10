package zbl.moonlight.raft.state;

import zbl.moonlight.raft.request.Entry;

import java.nio.channels.SelectionKey;

/**
 * Raft 定义的状态机的接口
 */
public interface StateMachine {
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
