package zbl.moonlight.client;

import zbl.moonlight.core.raft.response.RaftResponse;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.response.SocketResponse;

import java.nio.ByteBuffer;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static zbl.moonlight.client.Command.GET_COMMAND;
import static zbl.moonlight.client.Command.SET_COMMAND;

public class ClientHandler implements SocketClientHandler {
    private final CyclicBarrier barrier;

    ClientHandler(CyclicBarrier barrier) {
        this.barrier = barrier;
    }

    public void handleConnected(ServerNode node) throws Exception {
        Printer.printConnected(node);
        barrier.await();
    }

    public void handleResponse(SocketResponse response) throws BrokenBarrierException, InterruptedException {
        byte[] data = response.data();

        if(data == null || data.length < 1) {
            throw new RuntimeException("Response data can not be null");
        }

        String commandName = (String) response.attachment();
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte status = buffer.get();
        byte[] body = new byte[data.length - 1];
        buffer.get(body);

        switch (commandName) {
            case GET_COMMAND -> {
                if(body.length == 0) {
                    Printer.printValueNotExist();
                } else {
                    Printer.printValue(body);
                }
            }

            case SET_COMMAND -> {
                if(status == RaftResponse.CLIENT_REQUEST_SUCCESS) {
                    Printer.printOK();
                }
            }
        }
        barrier.await();
    }
}
