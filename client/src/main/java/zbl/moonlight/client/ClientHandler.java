package zbl.moonlight.client;

import lombok.Setter;
import zbl.moonlight.client.printer.Printer;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.result.Result;
import zbl.moonlight.socket.interfaces.SocketClientHandler;
import zbl.moonlight.socket.response.SocketResponse;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class ClientHandler implements SocketClientHandler {
    private final CyclicBarrier barrier;
    @Setter
    private MoonlightClient client;

    ClientHandler(CyclicBarrier barrier) {
        this.barrier = barrier;
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) throws Exception {
        Printer.printConnected(((SocketChannel)selectionKey.channel()).getRemoteAddress());
        barrier.await();
    }

    @Override
    public void handleResponse(SocketResponse response) throws BrokenBarrierException, InterruptedException {
        int serial = response.serial();
        byte[] data = response.data();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte code = buffer.get();

        switch (code) {
            case Result.SUCCESS -> Printer.printOK();
            case Result.Error.INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                Printer.printError(message);
            }

            default -> Printer.printError("Unknown Response Status Code");
        }

        barrier.await();
    }

    @Override
    public void handleConnectFailure(SelectionKey selectionKey) throws Exception {
        String message = String.format("Connect to [%s] failure", ((SocketChannel)selectionKey.channel()).getRemoteAddress());
        Printer.printError(message);
        /* 清空客户端的当前节点 */
        client.setCurrent(null);
        barrier.await();
    }
}
