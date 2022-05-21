package zbl.moonlight.client;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.request.SocketRequest;

import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static zbl.moonlight.client.Command.*;

public class MoonlightClient extends Shutdown {
    private final SocketClient socketClient;
    private final Scanner scanner;

    private final CyclicBarrier barrier = new CyclicBarrier(2);

    /**
     * 终端当前连接的节点
     */
    private ServerNode current;


    public MoonlightClient() throws IOException {
        socketClient = new SocketClient();
        socketClient.setHandler(new ClientHandler(barrier));
        scanner = new Scanner(System.in);
    }

    public void start() throws BrokenBarrierException, InterruptedException, IOException {
        Executor.start(socketClient);

        while (!isShutdown()) {
            Printer.printPrompt(current);
            Command command = Command.fromString(scanner.nextLine());

            switch (command.name()) {
                case CONNECT_COMMAND -> {
                    current = new ServerNode(command.key(),
                            Integer.parseInt(command.value()));
                    socketClient.connect(current);
                    socketClient.interrupt();

                    /* 等待连接成功 */
                    barrier.await();
                }

                case GET_COMMAND, SET_COMMAND, DELETE_COMMAND -> {
                }

                case CLUSTER_COMMAND -> {

                }

                default -> {
                    String error = String.format("Invalid command [%s]", command.name());
                    Printer.printError(error);
                }
            }
        }
    }

    public static void main(String[] args) throws IOException,
            BrokenBarrierException, InterruptedException {
        MoonlightClient client = new MoonlightClient();
        client.start();
    }

    private void send(Command command) {
        SocketRequest request = SocketRequest.newUnicastRequest(command.toBytes(), current);
        socketClient.offerInterruptibly(request);
    }
}
