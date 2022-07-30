package zbl.moonlight.client;

import lombok.Setter;
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
        byte[] data = response.data();

        if(data == null || data.length < 1) {
            throw new RuntimeException("Response data can not be null");
        }

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte status = buffer.get();
        byte[] body = new byte[data.length - 1];
        buffer.get(body);

//        switch (commandName) {
//            case GET_COMMAND -> {
//                if(body.length == 0) {
//                    Printer.printValueNotExist();
//                } else {
//                    Printer.printValue(body);
//                }
//            }
//
//            case SET_COMMAND, DELETE_COMMAND -> {
//                if(status == CLIENT_REQUEST_SUCCESS) {
//                    Printer.printOK();
//                }
//            }
//        }
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
