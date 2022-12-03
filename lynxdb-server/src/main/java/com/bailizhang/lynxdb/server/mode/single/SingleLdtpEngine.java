package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.CLIENT_REQUEST;

public class SingleLdtpEngine extends Executor<SocketRequest> {
    private static final Logger logger = LogManager.getLogger("SingleLdtpEngine");

    private final SocketServer server;
    private final LdtpStorageEngine engine = new LdtpStorageEngine();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

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

        CompletableFuture.runAsync(
                () -> {
                    QueryParams queryParams = QueryParams.parse(command);
                    QueryResult result = engine.doQuery(queryParams);

                    WritableSocketResponse response = new WritableSocketResponse(
                            request.selectionKey(),
                            request.serial(),
                            result.data(),
                            result.affectValues()
                    );

                    server.offerInterruptibly(response);
                },
                executor
        );
    }
}
