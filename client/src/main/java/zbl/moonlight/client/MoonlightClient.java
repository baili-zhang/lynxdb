package zbl.moonlight.client;

import lombok.Setter;
import zbl.moonlight.client.mql.MQL;
import zbl.moonlight.client.mql.MqlQuery;
import zbl.moonlight.core.common.Converter;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.raft.request.RaftRequest;
import zbl.moonlight.raft.response.ClientResult;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.client.SocketClient;
import zbl.moonlight.socket.request.WritableSocketRequest;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

public class MoonlightClient extends Shutdown {
    private final SocketClient socketClient;
    private final Scanner scanner;

    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private final ClientHandler clientHandler = new ClientHandler(barrier);

    private final AtomicInteger serial = new AtomicInteger(1);

    /**
     * 终端当前连接的节点
     */
    @Setter
    private volatile SelectionKey current;

    public MoonlightClient() throws IOException {
        socketClient = new SocketClient();
        socketClient.setHandler(clientHandler);
        scanner = new Scanner(System.in);

        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    public void start() throws IOException, BrokenBarrierException, InterruptedException {
        Executor.start(socketClient);
        clientHandler.setClient(this);

        SelectionKey selectionKey = socketClient
                .connect(new ServerNode("127.0.0.1", 7820));

        while (isNotShutdown()) {
            barrier.await();

            if(barrier.isBroken()) {
                barrier.reset();
            }

            Printer.printPrompt(current);
            String statement = scanner.nextLine();

            List<MqlQuery> queries = MQL.parse(statement);
            List<byte[]> total = new ArrayList<>();

            for(MqlQuery query : queries) {
                total.add(
                    switch (query.name()) {
                        case MQL.Keywords.CREATE -> handleCreate(query);
                        case MQL.Keywords.DELETE -> handleDelete(query);

                        default -> throw new UnsupportedOperationException(query.name());
                    }
                );
            }

            byte[] queryBytes = total.get(0);

            socketClient.sendMessage(selectionKey, new ClientRequest(queryBytes).toBytes());
        }
    }

    private byte[] handleDelete(MqlQuery query) {
        return new byte[0];
    }

    private byte[] handleCreate(MqlQuery query) {
        switch (query.type()) {
            case MQL.Keywords.KVSTORE -> {
                return new byte[0];
            }

            case MQL.Keywords.TABLE -> {
                byte method = MdtpMethod.CREATE_TABLE;
                List<byte[]> tables = query.tables().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * tables.size());
                tables.forEach(table -> length.getAndAdd(table.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                tables.forEach(table -> buffer.putInt(table.length).put(table));

                return buffer.array();
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    public static void main(String[] args)
            throws IOException, BrokenBarrierException, InterruptedException {

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

    private void disconnect() {
        Printer.printDisconnect(current);
        current = null;
    }
}
