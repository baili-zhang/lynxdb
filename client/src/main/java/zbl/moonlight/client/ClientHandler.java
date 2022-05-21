package zbl.moonlight.client;

import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.interfaces.SocketClientHandler;
import zbl.moonlight.core.socket.response.SocketResponse;

import java.util.concurrent.CyclicBarrier;

public class ClientHandler implements SocketClientHandler {
    private final CyclicBarrier barrier;

    ClientHandler(CyclicBarrier barrier) {
        this.barrier = barrier;
    }

    public void handleConnected(ServerNode node) throws Exception {
        Printer.printConnected(node);
        barrier.await();
    }
    public void handleResponse(SocketResponse response) throws Exception {

    }
}
