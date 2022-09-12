package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.raft.common.RaftLogEntry;
import com.bailizhang.lynxdb.raft.common.RaftSnapshot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.raft.common.RaftCommand;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.List;

import static com.bailizhang.lynxdb.raft.common.RaftLogEntry.CLUSTER_MEMBERSHIP_CHANGE;
import static com.bailizhang.lynxdb.raft.common.RaftLogEntry.DATA_CHANGE;

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
    public RaftSnapshot currentSnapshot() {
        RaftSnapshot snapshot = new RaftSnapshot();

        List<String> kvstores = storageEngine.allKvstores();
        for(String kvstore : kvstores) {
            byte[] data = storageEngine.kvstoreData(kvstore);
            snapshot.kvstore(kvstore, data);
        }

        List<String> tables = storageEngine.allTables();
        for(String table : tables) {
            byte[] data = storageEngine.tableData(table);
            snapshot.table(table, data);
        }

        return snapshot;
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
