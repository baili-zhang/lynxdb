package com.bailizhang.lynxdb.server.mode.single;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.mode.AffectKeyRegistry;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bailizhang.lynxdb.raft.request.RaftRequest.CLIENT_REQUEST;
import static com.bailizhang.lynxdb.server.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.server.mode.LynxDbServer.MESSAGE_SERIAL;
import static com.bailizhang.lynxdb.socket.code.Request.DEREGISTER_KEY;
import static com.bailizhang.lynxdb.socket.code.Request.REGISTER_KEY;

public class SingleLdtpEngine extends Executor<SocketRequest> {
    private static final Logger logger = LogManager.getLogger("SingleLdtpEngine");

    private final SocketServer server;
    private final LdtpStorageEngine engine = new LdtpStorageEngine();
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AffectKeyRegistry affectKeyRegistry = new AffectKeyRegistry();

    public SingleLdtpEngine(SocketServer socketServer) {
        server = socketServer;
    }

    @Override
    protected void execute() {
        SocketRequest request = blockPoll();

        if(request == null) {
            return;
        }

        SelectionKey selectionKey = request.selectionKey();
        int serial = request.serial();
        byte[] data = request.data();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte flag = buffer.get();

        switch (flag) {
            case CLIENT_REQUEST ->
                    // 防止数据库操作被中断
                    CompletableFuture.runAsync(
                            () -> {
                                QueryParams queryParams = QueryParams.parse(buffer);
                                QueryResult result = engine.doQuery(queryParams);

                                WritableSocketResponse response = new WritableSocketResponse(
                                        selectionKey,
                                        serial,
                                        result.data()
                                );

                                // 返回给发起请求的客户端
                                server.offerInterruptibly(response);

                                // 处理注册监听的 key
                                AffectKey affectKey = result.affectKey();
                                if(affectKey == null) {
                                    return;
                                }

                                List<SelectionKey> keys = affectKeyRegistry.selectionKeys(affectKey);
                                List<DbValue> dbValues = engine.find(
                                        affectKey.key(),
                                        affectKey.columnFamily()
                                );

                                AffectValue affectValue = new AffectValue(affectKey, dbValues);

                                for(SelectionKey key : keys) {
                                    WritableSocketResponse affectResponse = new WritableSocketResponse(
                                            key,
                                            MESSAGE_SERIAL,
                                            affectValue
                                    );

                                    // 返回修改的信息给注册监听的客户端
                                    server.offerInterruptibly(affectResponse);
                                }
                            },
                            executor
                    );

            case REGISTER_KEY -> {
                affectKeyRegistry.register(selectionKey, AffectKey.from(buffer));

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
            }
            case DEREGISTER_KEY -> {
                affectKeyRegistry.deregister(selectionKey, AffectKey.from(buffer));

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
            }

            default -> throw new RuntimeException();
        }
    }
}
