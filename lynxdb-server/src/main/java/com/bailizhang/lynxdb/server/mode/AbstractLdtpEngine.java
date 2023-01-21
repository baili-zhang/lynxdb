package com.bailizhang.lynxdb.server.mode;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutKey;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutValue;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bailizhang.lynxdb.server.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.server.mode.LynxDbServer.MESSAGE_SERIAL;
import static com.bailizhang.lynxdb.socket.code.Request.*;

public abstract class AbstractLdtpEngine extends Executor<SocketRequest> {
    private static final Logger logger = LogManager.getLogger("AbstractLdtpEngine");

    private final SocketServer server;
    private final LdtpStorageEngine engine;
    private final LynxDbTimeWheel timeWheel;
    private final ExecutorService executor = Executors.newSingleThreadExecutor();
    private final AffectKeyRegistry affectKeyRegistry = new AffectKeyRegistry();

    public AbstractLdtpEngine(SocketServer socketServer, LynxDbTimeWheel lynxDbTimeWheel) {
        server = socketServer;
        engine = new LdtpStorageEngine();
        timeWheel = lynxDbTimeWheel;
    }

    // TODO: 抽象到一个通用的执行器中
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
            case CLIENT_REQUEST -> CompletableFuture.runAsync(() -> {
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
                sendAffectValueToRegisterClient(affectKey);
            }, executor);

            case REGISTER_KEY -> CompletableFuture.runAsync(() -> {
                AffectKey affectKey = AffectKey.from(buffer);
                affectKeyRegistry.register(selectionKey, affectKey);

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
                sendAffectValueToRegisterClient(affectKey);
            }, executor);
            case DEREGISTER_KEY -> CompletableFuture.runAsync(() -> {
                affectKeyRegistry.deregister(selectionKey, AffectKey.from(buffer));

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
            }, executor);

            case SET_TIMEOUT_KEY -> CompletableFuture.runAsync(() -> {
                TimeoutValue timeoutValue = TimeoutValue.from(buffer);
                engine.insertTimeoutKey(timeoutValue);

                TimeoutKey timeoutKey = timeoutValue.timeoutKey();
                long timestamp = ByteArrayUtils.toLong(timeoutValue.value());

                TimeoutTask task = new TimeoutTask(timestamp, timeoutKey, () -> {
                    sendTimeoutValueToClient(timeoutKey, selectionKey);
                    engine.removeTimeoutKey(timeoutKey);
                    engine.removeData(timeoutKey);
                });

                timeWheel.register(task);

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
            }, executor);

            case REMOVE_TIMEOUT_KEY -> CompletableFuture.runAsync(() -> {
                TimeoutKey timeoutKey = TimeoutKey.from(buffer);

                byte[] value = engine.findTimeoutValue(timeoutKey);
                long timestamp = ByteArrayUtils.toLong(value);
                engine.removeTimeoutKey(timeoutKey);

                timeWheel.unregister(timestamp, timeoutKey);

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
            }, executor);

            default -> throw new RuntimeException();
        }
    }

    private void sendAffectValueToRegisterClient(AffectKey affectKey) {
        List<SelectionKey> keys = affectKeyRegistry.selectionKeys(affectKey);
        List<DbValue> dbValues = engine.findAffectKey(affectKey);

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
    }

    private void sendTimeoutValueToClient(TimeoutKey timeoutKey, SelectionKey selectionKey) {
        byte[] value = engine.findTimeoutValue(timeoutKey);

        TimeoutValue timeoutValue = new TimeoutValue(timeoutKey, value);

        WritableSocketResponse affectResponse = new WritableSocketResponse(
                selectionKey,
                MESSAGE_SERIAL,
                timeoutValue
        );

        // 返回修改的信息给注册监听的客户端
        server.offerInterruptibly(affectResponse);
    }
}
