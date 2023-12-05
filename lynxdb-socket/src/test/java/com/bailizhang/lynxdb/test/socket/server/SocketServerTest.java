package com.bailizhang.lynxdb.test.socket.server;

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

class SocketServerTest {
    private final byte[] requestData = "request".repeat(1).getBytes(StandardCharsets.UTF_8);
    private final byte[] responseData = "response".repeat(1).getBytes(StandardCharsets.UTF_8);

    private final int REQUEST_COUNT = 20;
    private final int CLIENT_COUNT = 1;

    private final int requestSerial = 15;
    private final int responseSerial = 20;

    @Test
    void execute() throws Exception {
        SocketServer server = new SocketServer(new SocketServerConfig(7820));
        server.setHandler(new SocketServerHandler() {
            @Override
            public void handleRequest(SegmentSocketRequest request) throws Exception {
                // TODO
                // assert Arrays.equals(request.data(), requestData);

                DataBlocks dataBlocks = new DataBlocks(false);
                dataBlocks.appendRawBytes(responseData);

                System.out.println("Accept client request");

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