package com.bailizhang.lynxdb.server.mode;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.health.FlightDataRecorder;
import com.bailizhang.lynxdb.server.engine.LdtpStorageEngine;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.LockSupport;

import static com.bailizhang.lynxdb.ldtp.request.RequestType.FLIGHT_RECORDER;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.LDTP_METHOD;

public class LdtpEngineExecutor extends Executor<SocketRequest> {
    private static final Logger logger = LoggerFactory.getLogger(LdtpEngineExecutor.class);

    private static final String TASKS_THREAD_NAME = "tasks-thread";

    private final SocketServer server;
    private final LdtpStorageEngine engine;

    private final ConcurrentLinkedQueue<Runnable> tasksQueue;
    private final Thread tasksThread;

    public LdtpEngineExecutor(SocketServer socketServer) {
        server = socketServer;
        engine = new LdtpStorageEngine();
        tasksQueue = new ConcurrentLinkedQueue<>();
        tasksThread = new Thread(this::runTask, TASKS_THREAD_NAME);
    }

    @Override
    protected void doBeforeExecute() {
        tasksThread.start();
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
            case LDTP_METHOD -> {
                tasksQueue.offer(() -> handleLdtpMethod(selectionKey, serial, buffer));
                LockSupport.unpark(tasksThread);
            }

            case FLIGHT_RECORDER -> {
                tasksQueue.offer(() -> handleDataRecorder(selectionKey, serial, buffer));
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

    private void handleLdtpMethod(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        QueryParams queryParams = QueryParams.parse(buffer);

        logger.info("Handle client request, params: {}", queryParams);

        QueryResult result = engine.doQuery(queryParams);

        logger.debug("Result is: {}", result);

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                result.data().toBuffers()
        );

        logger.info("Offer response to server executor, {}", response);

        // 返回给发起请求的客户端
        server.offerInterruptibly(response);
    }

    private void handleDataRecorder(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        assert !buffer.hasRemaining();

        FlightDataRecorder recorder = FlightDataRecorder.recorder();
        var data = recorder.data();

        DataBlocks dataBlocks = new DataBlocks();
        data.forEach(pair -> {
            dataBlocks.appendVarStr(pair.left().name());
            dataBlocks.appendRawByte(pair.left().unit().value());
            dataBlocks.appendRawLong(pair.right());
        });

        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                dataBlocks.toBuffers()
        );

        server.offerInterruptibly(response);
    }

}
