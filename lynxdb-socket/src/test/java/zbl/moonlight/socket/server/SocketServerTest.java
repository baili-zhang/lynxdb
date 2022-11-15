package zbl.moonlight.socket.server;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executor;
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

                BytesList bytesList = new BytesList();
                bytesList.appendRawBytes(responseData);

                server.offerInterruptibly(new WritableSocketResponse(
                        request.selectionKey(),
                        responseSerial,
                        bytesList,
                        null));
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