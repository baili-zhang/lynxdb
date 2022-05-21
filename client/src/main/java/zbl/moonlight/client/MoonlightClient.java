package zbl.moonlight.client;

import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.raft.request.ClientRequest;
import zbl.moonlight.core.raft.request.RaftRequest;
import zbl.moonlight.core.socket.client.ServerNode;
import zbl.moonlight.core.socket.client.SocketClient;
import zbl.moonlight.core.socket.request.SocketRequest;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.server.mdtp.MdtpMethod;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static zbl.moonlight.client.Command.*;
import static zbl.moonlight.core.raft.request.ClientRequest.RAFT_CLIENT_REQUEST_GET;
import static zbl.moonlight.core.raft.request.ClientRequest.RAFT_CLIENT_REQUEST_SET;

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
                /* 处理连接命令 */
                case CONNECT_COMMAND -> {
                    current = new ServerNode(command.key(),
                            Integer.parseInt(command.value()));
                    socketClient.connect(current);
                    socketClient.interrupt();

                    /* 等待连接成功 */
                    barrier.await();
                }

                /* 处理 GET 命令 */
                case GET_COMMAND -> {
                    byte method = MdtpMethod.GET;
                    byte[] key = command.key().getBytes(StandardCharsets.UTF_8);
                    int len = NumberUtils.BYTE_LENGTH * 3 + NumberUtils.INT_LENGTH * 2
                            + key.length;

                    ByteBuffer buffer = ByteBuffer.allocate(len);
                    buffer.put(RaftRequest.CLIENT_REQUEST)
                            .put(RAFT_CLIENT_REQUEST_GET)
                            .put(method)
                            .putInt(key.length)
                            .put(key)
                            .putInt(0);

                    SocketRequest request = SocketRequest.newUnicastRequest(buffer.array(),
                            current, command.name());
                    socketClient.offerInterruptibly(request);

                    barrier.await();
                }

                /* 处理 SET 命令 */
                case SET_COMMAND -> {
                    byte method = MdtpMethod.SET;
                    byte[] key = command.key().getBytes(StandardCharsets.UTF_8);
                    byte[] value = command.value().getBytes(StandardCharsets.UTF_8);
                    int len = NumberUtils.BYTE_LENGTH * 3 + NumberUtils.INT_LENGTH * 2
                            + key.length + value.length;

                    ByteBuffer buffer = ByteBuffer.allocate(len);
                    buffer.put(RaftRequest.CLIENT_REQUEST)
                            .put(RAFT_CLIENT_REQUEST_SET)
                            .put(method)
                            .putInt(key.length)
                            .put(key)
                            .putInt(value.length)
                            .put(value);

                    SocketRequest request = SocketRequest.newUnicastRequest(buffer.array(),
                            current, command.name());
                    socketClient.offerInterruptibly(request);

                    barrier.await();
                }

                case DELETE_COMMAND -> {

                }

                case CLUSTER_COMMAND -> {

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
