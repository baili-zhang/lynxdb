package zbl.moonlight.core.socket;

import org.junit.jupiter.api.Test;
import zbl.moonlight.core.executor.Executable;
import zbl.moonlight.core.socket.interfaces.Callback;
import zbl.moonlight.core.socket.interfaces.RequestHandler;
import zbl.moonlight.core.socket.interfaces.SocketState;
import zbl.moonlight.core.socket.response.WritableSocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.socket.server.SocketServerConfig;
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
        RequestHandler handler = (request) -> {
            byte[] data = request.getData().array();
            assert new String(data).equals(req);
            server.offer(new WritableSocketResponse(request.selectionKey(), res.getBytes(StandardCharsets.UTF_8)));
        };

        Callback callback = new Callback() {
            @Override
            public void doAfterRunning() {
                try {
                    barrier.await();
                } catch (InterruptedException | BrokenBarrierException e) {
                    e.printStackTrace();
                }
            }
        };

        server.setHandler(handler);
        server.setCallback(callback);
        Executable.start(server);

        barrier.await();

        Socket socket = new Socket("127.0.0.1", port);

        OutputStream outputStream = socket.getOutputStream();
        byte[] data = req.getBytes(StandardCharsets.UTF_8);
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH
                + NumberUtils.BYTE_LENGTH + data.length);
        buffer.putInt(data.length).put(SocketState.STAY_CONNECTED).put(data);
        outputStream.write(buffer.array());
        outputStream.flush();

        InputStream inputStream = socket.getInputStream();
        int len = res.getBytes(StandardCharsets.UTF_8).length;
        byte[] bytes = new byte[NumberUtils.INT_LENGTH + len];
        inputStream.read(bytes);
        ByteBuffer buf = ByteBuffer.wrap(bytes);

        assert buf.getInt() == len;
        byte[] resData = new byte[len];
        buf.get(resData);
        assert new String(resData).equals(res);
    }
}