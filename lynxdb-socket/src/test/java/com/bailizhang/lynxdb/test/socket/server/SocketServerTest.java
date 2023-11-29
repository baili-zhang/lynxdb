package com.bailizhang.lynxdb.test.socket.server;

import com.bailizhang.lynxdb.core.common.DataBlocks;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;
import com.bailizhang.lynxdb.socket.interfaces.SocketClientHandler;
import com.bailizhang.lynxdb.socket.interfaces.SocketServerHandler;
import com.bailizhang.lynxdb.socket.request.SocketRequest;
import com.bailizhang.lynxdb.socket.request.WritableSocketRequest;
import com.bailizhang.lynxdb.socket.response.SocketResponse;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.socket.server.SocketServer;
import com.bailizhang.lynxdb.socket.server.SocketServerConfig;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

class SocketServerTest {

    private final byte[] requestData = "request".getBytes(StandardCharsets.UTF_8);
    private final byte[] responseData = "response".getBytes(StandardCharsets.UTF_8);

    private final int REQUEST_COUNT = 20000;
    private final int CLIENT_COUNT = 5;

    private final byte requestStatus = (byte) 0x03;

    private final int requestSerial = 15;
    private final int responseSerial = 20;

    @Test
    void execute() throws IOException, InterruptedException {
        SocketServer server = new SocketServer(new SocketServerConfig(7820));
        server.setHandler(new SocketServerHandler() {
            @Override
            public void handleRequest(SocketRequest request) throws Exception {
                assert request.status() == requestStatus;
                assert Arrays.equals(request.data(), requestData);

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
            clients[i] = new SocketClient();
            clients[i].setHandler(new SocketClientHandler() {
                @Override
                public void handleConnected(SelectionKey selectionKey) {
                    for(int i = 0; i < REQUEST_COUNT; i ++) {
                        clients[i].offerInterruptibly(new WritableSocketRequest(
                                selectionKey,
                                requestStatus,
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
        }

        // TODO 使用 Record 记录
        long begin = System.currentTimeMillis();

        for(int i = 0; i < CLIENT_COUNT; i ++) {
            Executor.start(clients[i]);
            clients[i].connect(new ServerNode("127.0.0.1", 7820));
        }

        latch.await();

        long end = System.currentTimeMillis();

        System.out.println(end - begin);
    }
}