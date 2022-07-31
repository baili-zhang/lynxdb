package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.log.RaftLogEntry;
import zbl.moonlight.raft.server.RaftServer;
import zbl.moonlight.raft.state.RaftCommand;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.server.engine.MdtpStorageEngine;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.response.WritableSocketResponse;

import java.util.List;

import static zbl.moonlight.raft.state.RaftState.CLUSTER_MEMBERSHIP_CHANGE;
import static zbl.moonlight.raft.state.RaftState.DATA_CHANGE;
import static zbl.moonlight.server.engine.MdtpStorageEngine.C;

/**
 * TODO: 异步执行会不会存在数据丢失的问题？
 *
 * 客户端 -> Raft 层 -> 状态机 -> Raft 层 -> 客户端
 */
public class MdtpStateMachine extends Executor<RaftCommand> implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    /**
     * 定义成 static final 类型，无论实例化多少个 MdtpStateMachine，都只有一个 MdtpStorageEngine 实例
     */
    private static final MdtpStorageEngine storageEngine = new MdtpStorageEngine();

    private final RaftServer raftServer;

    public MdtpStateMachine(RaftServer server) {
        raftServer = server;
    }

    @Override
    public List<ServerNode> clusterNodes() {
        byte[] cOldNew = storageEngine.metaGet(C);

        if(cOldNew == null) {
            byte[] c = storageEngine.metaGet(C);
            return ServerNode.parseNodeList(c);
        }

        return ServerNode.parseNodeList(cOldNew);
    }

    @Override
    public void apply(RaftLogEntry[] entries) {
        for (RaftLogEntry entry : entries) {
            switch (entry.type()) {
                // 处理数据改变
                case DATA_CHANGE -> {
                    // 执行查询操作
                    byte[] data = storageEngine.doQuery(entry.command());
                    // 构建 Socket 响应对象
                    WritableSocketResponse response = new WritableSocketResponse(
                            entry.selectionKey(),
                            entry.serial(),
                            data
                    );
                    // 发送响应
                    raftServer.offerInterruptibly(response);
                }

                // 处理集群成员改变
                case CLUSTER_MEMBERSHIP_CHANGE -> {
                    storageEngine.metaSet(C, entry.command());
                }
            }
        }
    }

    @Override
    protected void execute() {

    }
}
