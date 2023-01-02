package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.raft.common.RaftCommend;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * TODO: 异步执行会不会存在数据丢失的问题？
 *
 * 客户端 -> Raft 层 -> 状态机 -> Raft 层 -> 客户端
 */
public class LdtpStateMachine implements StateMachine {
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
    }

    @Override
    public List<ServerNode> clusterNodes() {
        return null;
    }

    @Override
    public void apply(List<RaftCommend> entries) {
        for (RaftCommend entry : entries) {
            ByteBuffer buffer = ByteBuffer.wrap(entry.data());
            QueryParams params = QueryParams.parse(buffer);
            QueryResult result = storageEngine.doQuery(params);
            WritableSocketResponse response = new WritableSocketResponse(
                    entry.selectionKey(),
                    entry.serial(),
                    result.data()
            );
            raftServer.offerInterruptibly(response);
        }
    }

    @Override
    public void apply0(List<byte[]> commands) {
        for(byte[] command : commands) {
            ByteBuffer buffer = ByteBuffer.wrap(command);
            QueryParams params = QueryParams.parse(buffer);
            storageEngine.doQuery(params);
        }
    }
}
