package zbl.moonlight.core.socket.client;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.interfaces.SocketServerHandler;
import zbl.moonlight.core.socket.interfaces.SocketState;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

class SocketClientTest {
    private final static List<ServerNode> serverNodes;

    static {
        serverNodes = new ArrayList<>();
        String host = "127.0.0.1";
        for (int i = 0; i < 5; i++) {
            serverNodes.add(new ServerNode(host, 7821 + i));
        }
    }

    @Test
    void testBroadcast() throws IOException, BrokenBarrierException, InterruptedException {
        CyclicBarrier barrier = new CyclicBarrier(serverNodes.size() + 1);
        CountDownLatch latch = new CountDownLatch(serverNodes.size());

        String requestBody = "PING";
        String responsePrefix = "PONG";

        for (ServerNode node : serverNodes) {
            SocketServer server = new SocketServer(new SocketServerConfig(node.port()));
            server.setHandler(new SocketServerHandler() {
                @Override
                public void handleStartupCompleted() {
                    try {
                        barrier.await();
                    } catch (InterruptedException | BrokenBarrierException e) {
                        e.printStackTrace();
                    }
                }

                @Override
                public void handleRequest(SocketRequest request) {
                    byte[] data = request.data();
                    assert new String(data).equals(requestBody);
                    byte[] res = (responsePrefix + node).getBytes(StandardCharsets.UTF_8);
                    server.offer(new SocketResponse(request.selectionKey(), res));
                }
            });
            Executor.start(server);
        }

        barrier.await();

        SocketClient client = new SocketClient();
        client.setHandler(new SocketClientHandler() {
            @Override
            public void handleConnected() {
                latch.countDown();
            }

            @Override
            public void handleResponse(SocketResponse response) {
                /* TODO: 使用 Assert 判断 */
                System.out.println(new String(response.data()));
            }
        });

        Executor.start(client);
        for (ServerNode node : serverNodes) {
            client.connect(node);
        }

        latch.await();

        byte[] data = requestBody.getBytes(StandardCharsets.UTF_8);
        byte status = SocketState.STAY_CONNECTED_FLAG | SocketState.BROADCAST_FLAG;
        client.offerInterruptibly(SocketRequest.newBroadcastRequest(status, data));
    }
}