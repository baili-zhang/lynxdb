package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.common.AppliableLogEntry;
import com.bailizhang.lynxdb.raft.common.RaftCommand;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static com.bailizhang.lynxdb.raft.common.AppliableLogEntry.CLIENT_COMMAND;
import static com.bailizhang.lynxdb.raft.common.AppliableLogEntry.MEMBER_CHANGE;

/**
 * TODO: 异步执行会不会存在数据丢失的问题？
 *
 * 客户端 -> Raft 层 -> 状态机 -> Raft 层 -> 客户端
 */
public class LdtpStateMachine extends Executor<RaftCommand> implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    public static final String C_OLD_NEW = "c_old_new";

    private static final LdtpStorageEngine storageEngine = new LdtpStorageEngine();

    private RaftServer raftServer;

    public LdtpStateMachine() {
    }

    public void raftServer(RaftServer server) {
        raftServer = server;
    }

    @Override
    public void metaSet(String key, byte[] value) {
        storageEngine.metaSet(key, value);
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
    public void apply(AppliableLogEntry[] entries) {
        for (AppliableLogEntry entry : entries) {
            switch (entry.type()) {
                case CLIENT_COMMAND -> {
                    byte[] command = entry.data();
                    QueryParams params = QueryParams.parse(command);
                    BytesList bytesList = storageEngine.doQuery(params);
                    WritableSocketResponse response = new WritableSocketResponse(
                            entry.selectionKey(),
                            entry.serial(),
                            bytesList
                    );
                    raftServer.offerInterruptibly(response);
                }

                case MEMBER_CHANGE -> {
                    storageEngine.metaSet(C_OLD_NEW, entry.data());
                }
            }
        }
    }

    @Override
    protected void execute() {

    }
}
