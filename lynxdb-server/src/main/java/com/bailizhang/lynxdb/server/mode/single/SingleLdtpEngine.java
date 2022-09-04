package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.raft.request.ClientRequest;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.QueryParams;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.CLIENT_REQUEST;

public class SingleLdtpEngine extends Executor<SocketRequest> {
    private final SocketServer server;
    private final LdtpStorageEngine engine = new LdtpStorageEngine();

    public SingleLdtpEngine(SocketServer socketServer) {
        server = socketServer;
    }

    @Override
    protected void execute() {
        SocketRequest request = blockPoll();

        if(request == null) {
            return;
        }

        byte[] data = request.data();
        if(data[0] != CLIENT_REQUEST) {
            throw new RuntimeException("Request is not CLIENT_REQUEST");
        }

        byte[] command = new byte[data.length - 1];

        System.arraycopy(data, 1, command, 0, command.length);
        QueryParams queryParams = QueryParams.parse(command);
        BytesList result = engine.doQuery(queryParams);

        WritableSocketResponse response = new WritableSocketResponse(
                request.selectionKey(),
                request.serial(),
                result
        );

        server.offerInterruptibly(response);
    }
}
