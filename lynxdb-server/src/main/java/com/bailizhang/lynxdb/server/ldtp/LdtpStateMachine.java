package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.raft.state.RaftLogEntry;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.raft.state.RaftCommand;
import com.bailizhang.lynxdb.raft.state.StateMachine;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;

import java.util.List;

import static com.bailizhang.lynxdb.raft.state.RaftState.CLUSTER_MEMBERSHIP_CHANGE;
import static com.bailizhang.lynxdb.raft.state.RaftState.DATA_CHANGE;

/**
 * TODO: 异步执行会不会存在数据丢失的问题？
 *
 * 客户端 -> Raft 层 -> 状态机 -> Raft 层 -> 客户端
 */
public class LdtpStateMachine extends Executor<RaftCommand> implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    public static final String C_OLD_NEW = "c_old_new";

    /**
     * 定义成 static final 类型，无论实例化多少个 MdtpStateMachine，都只有一个 MdtpStorageEngine 实例
     */
    private static final LdtpStorageEngine storageEngine = new LdtpStorageEngine();

    private RaftServer raftServer;

    public LdtpStateMachine() {
    }

    public void raftServer(RaftServer server) {
        raftServer = server;
    }

    @Override
    public List<ServerNode> clusterNodes() {
        byte[] cOldNew = storageEngine.metaGet(C_OLD_NEW);

        if(cOldNew == null) {
            byte[] c = storageEngine.metaGet(C_OLD_NEW);
            return ServerNode.parseNodeList(c);
        }

        return ServerNode.parseNodeList(cOldNew);
    }

    @Override
    public void apply(RaftLogEntry[] entries) {
        for (RaftLogEntry entry : entries) {
            switch (entry.type()) {
                case DATA_CHANGE -> {
                    byte[] command = entry.command();
                    QueryParams params = QueryParams.parse(command);
                    byte[] data = storageEngine.doQuery(params);

                    WritableSocketResponse response = new WritableSocketResponse(
                            entry.selectionKey(),
                            entry.serial(),
                            data
                    );

                    raftServer.offerInterruptibly(response);
                }

                case CLUSTER_MEMBERSHIP_CHANGE -> {
                    storageEngine.metaSet(C_OLD_NEW, entry.command());
                }
            }
        }
    }

    @Override
    protected void execute() {

    }
}
