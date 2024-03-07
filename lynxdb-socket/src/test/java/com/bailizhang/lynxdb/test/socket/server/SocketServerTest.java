package com.bailizhang.lynxdb.test.socket.server;

import com.bailizhang.lynxdb.core.arena.Segment;
import com.bailizhang.lynxdb.core.buffers.Buffers;
import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.recorder.Recorder;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.ByteBufferSocketRequest;
import com.bailizhang.lynxdb.socket.request.SegmentSocketRequest;
import com.bailizhang.lynxdb.socket.response.SocketResponse;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import org.junit.jupiter.api.Test;

import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

class SocketServerTest {
    private final byte[] requestData = "request".repeat(1001).getBytes(StandardCharsets.UTF_8);
    private final byte[] responseData = "response".repeat(10).getBytes(StandardCharsets.UTF_8);

    private final int REQUEST_COUNT = 2000;
    private final int CLIENT_COUNT = 10;

    private final int requestSerial = 15;
    private final int responseSerial = 20;

    @Test
    void execute() throws Exception {
        SocketServer server = new SocketServer(new SocketServerConfig(7820));
        server.setHandler(new SocketServerHandler() {
            private final AtomicInteger i = new AtomicInteger(0);
            @Override
            public void handleRequest(SegmentSocketRequest request) {
                Segment[] data = request.data();
                Buffers buffers = Segment.buffers(data);
                byte[] rawData = buffers.toBytes();

                System.out.println(i.getAndIncrement());
                assert Arrays.equals(rawData, requestData);

                Segment.deallocAll(data);

                DataBlocks dataBlocks = new DataBlocks(false);
                dataBlocks.appendRawBytes(responseData);

                server.offerInterruptibly(new WritableSocketResponse(
                        request.selectionKey(),
                        responseSerial,
                        dataBlocks.toBuffers()));
            }
        });
        Executor.start(server);

        CountDownLatch latch = new CountDownLatch(REQUEST_COUNT * CLIENT_COUNT);

        SocketClient[] clients = new SocketClient[CLIENT_COUNT];
        for(int i = 0; i < CLIENT_COUNT; i ++) {
            SocketClient client = new SocketClient();
            client.setHandler(new SocketClientHandler() {
                @Override
                public void handleConnected(SelectionKey selectionKey) {
                    for(int j = 0; j < REQUEST_COUNT; j ++) {
                        client.offerInterruptibly(new ByteBufferSocketRequest(
                                selectionKey,
                                requestSerial,
                                BufferUtils.toBuffers(requestData)));
                    }
                }

                @Override
                public void handleResponse(SocketResponse response) {
                    assert response.serial() == responseSerial;
                    assert Arrays.equals(response.data(), responseData);
                    latch.countDown();
                }
            });

            clients[i] = client;
        }

        long runningTime = Recorder.runningTime(() -> {
            for(int i = 0; i < CLIENT_COUNT; i ++) {
                Executor.start(clients[i]);
                clients[i].connect(new ServerNode("127.0.0.1", 7820));
            }

            latch.await();
            return null;
        });

        System.out.println(runningTime);
    }
}