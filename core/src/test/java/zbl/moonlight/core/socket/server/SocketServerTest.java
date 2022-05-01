package zbl.moonlight.core.socket.server;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.socket.interfaces.SocketServerHandler;
import zbl.moonlight.core.socket.interfaces.SocketState;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.utils.NumberUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

class SocketServerTest {
    @Test
    void testSocketServer() throws IOException, BrokenBarrierException, InterruptedException {
        int port = 7820;
        String req = "PING";
        String res = "PONG";

        CyclicBarrier barrier = new CyclicBarrier(2);

        SocketServer server = new SocketServer(new SocketServerConfig(port));
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
                assert new String(data).equals(req);
                server.offer(new SocketResponse(request.selectionKey(),
                        res.getBytes(StandardCharsets.UTF_8), null));
            }
        });

        Executor.start(server);

        barrier.await();

        Socket socket = new Socket("127.0.0.1", port);

        OutputStream outputStream = socket.getOutputStream();
        byte[] data = req.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH
                + NumberUtils.BYTE_LENGTH + data.length);
        buffer.putInt(data.length).put(SocketState.STAY_CONNECTED_FLAG).put(data);
        outputStream.write(buffer.array());
        outputStream.flush();

        InputStream inputStream = socket.getInputStream();
        int len = res.getBytes(StandardCharsets.UTF_8).length;
        byte[] bytes = new byte[NumberUtils.INT_LENGTH];
        inputStream.read(bytes);
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        assert buf.getInt() == len;
        byte[] resData = new byte[len];
        inputStream.read(resData);
        assert new String(resData).equals(res);
    }
}