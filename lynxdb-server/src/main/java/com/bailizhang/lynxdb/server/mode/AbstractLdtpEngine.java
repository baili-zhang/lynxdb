package com.bailizhang.lynxdb.server.mode;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.ldtp.affect.AffectValue;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutValue;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.timewheel.LynxDbTimeWheel;
import com.bailizhang.lynxdb.timewheel.task.TimeoutTask;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.MESSAGE_SERIAL;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.socket.code.Request.*;

public abstract class AbstractLdtpEngine extends Executor<SocketRequest> {
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
                MessageKey messageKey = result.messageKey();
                if(messageKey == null) {
                    return;
                }
                sendAffectValueToRegisterClient(messageKey);
            }, executor);

            case REGISTER_KEY -> CompletableFuture.runAsync(() -> {
                MessageKey messageKey = MessageKey.from(buffer);
                affectKeyRegistry.register(selectionKey, messageKey);

                BytesList bytesList = new BytesList();
                bytesList.appendRawByte(VOID);

                WritableSocketResponse response = new WritableSocketResponse(
                        selectionKey,
                        serial,
                        bytesList
                );

                server.offerInterruptibly(response);
                sendAffectValueToRegisterClient(messageKey);
            }, executor);
            case DEREGISTER_KEY -> CompletableFuture.runAsync(() -> {
                affectKeyRegistry.deregister(selectionKey, MessageKey.from(buffer));

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

                MessageKey messageKey = timeoutValue.messageKey();
                long timestamp = ByteArrayUtils.toLong(timeoutValue.value());

                TimeoutTask task = new TimeoutTask(timestamp, messageKey, () -> {
                    sendTimeoutValueToClient(messageKey, selectionKey);
                    engine.removeTimeoutKey(messageKey);
                    engine.removeData(messageKey);
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
                MessageKey messageKey = MessageKey.from(buffer);

                byte[] value = engine.findTimeoutValue(messageKey);
                long timestamp = ByteArrayUtils.toLong(value);
                engine.removeTimeoutKey(messageKey);

                timeWheel.unregister(timestamp, messageKey);

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

    private void sendAffectValueToRegisterClient(MessageKey messageKey) {
        List<SelectionKey> keys = affectKeyRegistry.selectionKeys(messageKey);
        List<DbValue> dbValues = engine.findAffectKey(messageKey);

        AffectValue affectValue = new AffectValue(messageKey, dbValues);

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

    private void sendTimeoutValueToClient(MessageKey messageKey, SelectionKey selectionKey) {
        byte[] value = engine.findTimeoutValue(messageKey);

        TimeoutValue timeoutValue = new TimeoutValue(messageKey, value);

        WritableSocketResponse affectResponse = new WritableSocketResponse(
                selectionKey,
                MESSAGE_SERIAL,
                timeoutValue
        );

        // 返回修改的信息给注册监听的客户端
        server.offerInterruptibly(affectResponse);
    }
}
