package com.bailizhang.lynxdb.server.mode;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.ldtp.affect.AffectValue;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutValue;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.timewheel.SocketTimeWheel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.MESSAGE_SERIAL;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.socket.code.Request.*;

public class LdtpEngineExecutor extends Executor<SocketRequest> {
    private static final Logger logger = LoggerFactory.getLogger(LdtpEngineExecutor.class);

    private static final String TASKS_THREAD_NAME = "tasks-thread";

    private final SocketServer server;
    private final LdtpStorageEngine engine;
    private final SocketTimeWheel timeWheel;

    private final ConcurrentLinkedQueue<Runnable> tasksQueue;
    private final Thread tasksThread;

    private final AffectKeyRegistry affectKeyRegistry;

    public LdtpEngineExecutor(SocketServer socketServer) {
        server = socketServer;
        engine = new LdtpStorageEngine();
        timeWheel = SocketTimeWheel.timeWheel();
        tasksQueue = new ConcurrentLinkedQueue<>();
        affectKeyRegistry = new AffectKeyRegistry();
        tasksThread = new Thread(this::runTask, TASKS_THREAD_NAME);
    }

    @Override
    protected void doBeforeExecute() {
        tasksThread.start();
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
            case CLIENT_REQUEST -> {
                tasksQueue.offer(() -> handleClientRequest(selectionKey, serial, buffer));
                LockSupport.unpark(tasksThread);
            }

            case REGISTER_KEY -> {
                tasksQueue.offer(() -> handleRegisterKey(selectionKey, serial, buffer));
                LockSupport.unpark(tasksThread);
            }
            case DEREGISTER_KEY -> {
                tasksQueue.offer(() -> handleDeregisterKey(selectionKey, serial, buffer));
                LockSupport.unpark(tasksThread);
            }

            case SET_TIMEOUT_KEY -> {
                tasksQueue.offer(() -> handleSetTimeoutKey(selectionKey, serial, buffer));
                LockSupport.unpark(tasksThread);
            }

            case REMOVE_TIMEOUT_KEY -> {
                tasksQueue.offer(() -> handleRemoveTimeoutKey(selectionKey, serial, buffer));
                LockSupport.unpark(tasksThread);
            }

            default -> throw new RuntimeException();
        }
    }

    private void runTask() {
        while (isNotShutdown()) {
            Runnable task = tasksQueue.poll();
            if(task == null) {
                LockSupport.park();
                continue;
            }

            task.run();
        }
    }

    private void handleClientRequest(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        QueryParams queryParams = QueryParams.parse(buffer);

        logger.info("Handle client request, params: {}", queryParams);

        QueryResult result = engine.doQuery(queryParams);

        logger.debug("Result is: {}", result);

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                result.data()
        );

        logger.info("Offer response to server executor, {}", response);

        // 返回给发起请求的客户端
        server.offerInterruptibly(response);

        // 处理注册监听的 dbKey
        MessageKey messageKey = result.messageKey();

        if(messageKey == null) {
            return;
        }

        sendAffectValueToRegisterClient(messageKey);
    }

    private void handleRegisterKey(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
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
    }

    private void handleDeregisterKey(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        affectKeyRegistry.deregister(selectionKey, MessageKey.from(buffer));

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                bytesList
        );

        server.offerInterruptibly(response);
    }

    private void handleSetTimeoutKey(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        TimeoutValue timeoutValue = TimeoutValue.from(buffer);
        engine.insertTimeoutKey(timeoutValue);

        MessageKey messageKey = timeoutValue.messageKey();
        long timestamp = ByteArrayUtils.toLong(timeoutValue.value());

        timeWheel.register(timestamp, messageKey, () -> {
            sendTimeoutValueToClient(messageKey, selectionKey);
            engine.removeTimeoutKey(messageKey);
            engine.removeData(messageKey);
        });

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                bytesList
        );

        server.offerInterruptibly(response);
    }

    private void handleRemoveTimeoutKey(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
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
    }

    private void sendAffectValueToRegisterClient(MessageKey messageKey) {
        List<SelectionKey> keys = affectKeyRegistry.selectionKeys(messageKey);
        HashMap<String, byte[]> multiColumns = engine.findAffectKey(messageKey);

        AffectValue affectValue = new AffectValue(messageKey, multiColumns);

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
