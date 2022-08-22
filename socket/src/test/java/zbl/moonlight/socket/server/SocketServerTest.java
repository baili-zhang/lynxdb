package zbl.moonlight.socket.server;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.client.SocketClient;
import zbl.moonlight.socket.interfaces.SocketClientHandler;
import zbl.moonlight.socket.interfaces.SocketServerHandler;
import zbl.moonlight.socket.request.SocketRequest;
import zbl.moonlight.socket.request.WritableSocketRequest;
import zbl.moonlight.socket.response.SocketResponse;
import zbl.moonlight.socket.response.WritableSocketResponse;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

class SocketServerTest {

    private final byte[] requestData = "request".getBytes(StandardCharsets.UTF_8);
    private final byte[] responseData = "response".getBytes(StandardCharsets.UTF_8);

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

                server.offerInterruptibly(new WritableSocketResponse(
                        request.selectionKey(),
                        responseSerial,
                        responseData));
            }
        });
        Executor.start(server);

        CountDownLatch latch = new CountDownLatch(1);

        SocketClient client = new SocketClient();
        client.setHandler(new SocketClientHandler() {
            @Override
            public void handleConnected(SelectionKey selectionKey) throws Exception {
                client.offerInterruptibly(new WritableSocketRequest(
                        selectionKey,
                        requestStatus,
                        requestSerial,
                        requestData));
            }

            @Override
            public void handleResponse(SocketResponse response) throws Exception {
                assert response.serial() == responseSerial;
                assert Arrays.equals(response.data(), responseData);
                latch.countDown();
            }
        });

        Executor.start(client);
        client.connect(new ServerNode("127.0.0.1", 7820));
        latch.await();
    }
}