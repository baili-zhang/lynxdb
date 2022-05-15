package zbl.moonlight.core.raft.state;

import zbl.moonlight.core.raft.request.Entry;

import java.nio.channels.SelectionKey;

/**
 * Raft 定义的状态机的抽象实现
 */
public abstract class StateMachine {
    /**
     * 应用日志条目
     * @param entries 日志条目
     */
    public abstract void apply(Entry[] entries);

    /**
     * 执行客户端命令请求
     * @param command 命令
     */
    public abstract void exec(SelectionKey key, byte[] command);
}
