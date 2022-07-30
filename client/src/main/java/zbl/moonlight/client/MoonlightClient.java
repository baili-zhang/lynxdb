package zbl.moonlight.client;

import lombok.Setter;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.raft.request.RaftRequest;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.client.SocketClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static zbl.moonlight.client.Command.*;
import static zbl.moonlight.raft.request.ClientRequest.RAFT_CLIENT_REQUEST_GET;
import static zbl.moonlight.raft.request.ClientRequest.RAFT_CLIENT_REQUEST_SET;

public class MoonlightClient extends Shutdown {
    private final SocketClient socketClient;
    private final Scanner scanner;

    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private final ClientHandler clientHandler = new ClientHandler(barrier);

    /**
     * 终端当前连接的节点
     */
    @Setter
    private volatile ServerNode current;


    public MoonlightClient() throws IOException {
        socketClient = new SocketClient();
        socketClient.setHandler(clientHandler);
        scanner = new Scanner(System.in);
    }

    public void start() throws BrokenBarrierException, InterruptedException, IOException {
        Executor.start(socketClient);
        clientHandler.setClient(this);

        while (isNotShutdown()) {
            Printer.printPrompt(current);
            Command command = Command.fromString(scanner.nextLine());
            String commandName = command.name();

            if(current == null && isServerCommand(commandName)) {
                Printer.printNotConnectServer();
                continue;
            }

            switch (commandName) {
                /* 处理退出客户端命令 */
                case EXIT_COMMEND -> shutdown();

                /* 处理连接命令 */
                case CONNECT_COMMAND -> {
                    int port;

                    try {
                        port = Integer.parseInt(command.value());
                    } catch (NumberFormatException e) {
                        Printer.printError("Invalid [port]");
                        continue;
                    }

                    current = new ServerNode(command.key(), port);
                    socketClient.connect(current);
                    socketClient.interrupt();

                    /* 等待连接成功 */
                    barrier.await();
                }

                case DISCONNECT_COMMAND -> disconnect();

                /* 处理 GET 命令 */
                case GET_COMMAND -> {
                    barrier.await();
                }

                /* 处理 SET 命令 */
                case SET_COMMAND -> {
                    barrier.await();
                }

                /* 处理 DELETE 命令 */
                case DELETE_COMMAND -> {
                    barrier.await();
                }

                default -> {
                    String error = String.format("Invalid command [%s]", command.name());
                    Printer.printError(error);
                }
            }

            if(barrier.isBroken()) {
                barrier.reset();
            }
        }
    }

    private void send(byte method, Command command) {
        byte[] key = command.key().getBytes(StandardCharsets.UTF_8);
        byte[] value = command.value().getBytes(StandardCharsets.UTF_8);
        int len = NumberUtils.BYTE_LENGTH * 3 + NumberUtils.INT_LENGTH * 2
                + key.length + value.length;

        ByteBuffer buffer = ByteBuffer.allocate(len);
        buffer.put(RaftRequest.CLIENT_REQUEST);

        if(SET_COMMAND.equals(command.name()) || DELETE_COMMAND.equals(command.name())) {
            buffer.put(RAFT_CLIENT_REQUEST_SET);
        } else {
            buffer.put(RAFT_CLIENT_REQUEST_GET);
        }

        buffer.put(method)
                .putInt(key.length)
                .put(key)
                .putInt(value.length);

        if(value.length != 0) {
            buffer.put(value);
        }

//        WritableSocketRequest request = new WritableSocketRequest();
//        socketClient.offerInterruptibly(request);
    }

    private boolean isServerCommand(String commandName) {
        return GET_COMMAND.equals(commandName)
                || SET_COMMAND.equals(commandName)
                || DELETE_COMMAND.equals(commandName)
                || DISCONNECT_COMMAND.equals(commandName);
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
