package zbl.moonlight.client;

import lombok.Setter;
import zbl.moonlight.client.mql.MQL;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.raft.request.RaftRequest;
import zbl.moonlight.socket.client.SocketClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static zbl.moonlight.raft.request.ClientRequest.RAFT_CLIENT_REQUEST_GET;
import static zbl.moonlight.raft.request.ClientRequest.RAFT_CLIENT_REQUEST_SET;

public class MoonlightClient extends Shutdown {
    private final SocketClient socketClient;
    private final Scanner scanner;

    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private final ClientHandler clientHandler = new ClientHandler(barrier);

    private final AtomicInteger serial = new AtomicInteger(0);

    /**
     * 终端当前连接的节点
     */
    @Setter
    private volatile SelectionKey current;

    public MoonlightClient() throws IOException {
        socketClient = new SocketClient();
        socketClient.setHandler(clientHandler);
        scanner = new Scanner(System.in);
    }

    public void start() {
        Executor.start(socketClient);
        clientHandler.setClient(this);

        while (isNotShutdown()) {
            Printer.printPrompt(current);

            if(barrier.isBroken()) {
                barrier.reset();
            }
        }
    }

    private void send(byte method, MQL MQL) {

    }

    private void disconnect() {
        Printer.printDisconnect(current);
        current = null;
    }

    public static void main(String[] args) throws IOException,
            BrokenBarrierException, InterruptedException {
        MoonlightClient client = new MoonlightClient();
        client.start();
    }

    @Override
    protected void doAfterShutdown() {
        if(current != null) {
            disconnect();
        }
        socketClient.shutdown();
        socketClient.interrupt();
    }
}
