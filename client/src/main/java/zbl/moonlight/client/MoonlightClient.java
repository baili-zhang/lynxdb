package zbl.moonlight.client;

import lombok.Setter;
import zbl.moonlight.client.mql.MQL;
import zbl.moonlight.client.mql.MqlQuery;
import zbl.moonlight.client.printer.Printer;
import zbl.moonlight.core.common.Converter;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.raft.request.ClientRequest;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.client.SocketClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static zbl.moonlight.client.mql.MQL.Keywords.*;
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

            StringBuilder temp = new StringBuilder();
            String line;

            while (!(line = scanner.nextLine()).trim().endsWith(";")) {
                temp.append(" ").append(line);
            }

            temp.append(" ").append(line);
            String statement = temp.toString();

            List<MqlQuery> queries = MQL.parse(statement);
            List<byte[]> total = new ArrayList<>();

            for(MqlQuery query : queries) {
                total.add(
                    switch (query.name()) {
                        case CREATE -> handleCreate(query);
                        case DROP -> handleDrop(query);
                        case DELETE -> handleDelete(query);
                        case SHOW -> handleShow(query);
                        case SELECT -> handleSelect(query);
                        case INSERT -> TABLE.equalsIgnoreCase(query.type())
                                ? handleTableInsert(query) : handleKvInsert(query);

                        default -> throw new UnsupportedOperationException(query.name());
                    }
                );
            }

            byte[] queryBytes = total.get(0);

            socketClient.sendMessage(selectionKey, new ClientRequest(queryBytes).toBytes());
        }
    }

    private byte[] handleDrop(MqlQuery query) {
        switch (query.type()) {
            case KVSTORE -> {
                byte method = MdtpMethod.DROP_KV_STORE;
                List<byte[]> kvstores = query.kvstores().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * kvstores.size());
                kvstores.forEach(kvstore -> length.getAndAdd(kvstore.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(kvstores));

                return buffer.array();
            }

            case TABLE -> {
                byte method = MdtpMethod.DROP_TABLE;
                List<byte[]> tables = query.tables().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * tables.size());
                tables.forEach(table -> length.getAndAdd(table.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(tables));

                return buffer.array();
            }

            case COLUMNS -> {
                byte method = MdtpMethod.DROP_TABLE_COLUMN;

                byte[] table = G.I.toBytes(query.tables().get(0));
                List<byte[]> columns = query.columns().stream().map(G.I::toBytes).toList();

                List<byte[]> total = new ArrayList<>();
                total.add(table);
                total.addAll(columns);

                byte[] totalBytes = BufferUtils.toBytes(total);
                int length = BYTE_LENGTH + totalBytes.length;

                return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private byte[] handleTableInsert(MqlQuery query) {
        byte method = MdtpMethod.TABLE_INSERT;

        List<byte[]> keys = new ArrayList<>();
        List<byte[]> values = new ArrayList<>();

        List<byte[]> columns = query.columns().stream().map(G.I::toBytes).toList();

        for(List<String> row : query.rows()) {
            keys.add(G.I.toBytes(row.remove(0)));
            values.addAll(row.stream().map(G.I::toBytes).toList());
        }

        byte[] keysBytes = BufferUtils.toBytes(keys);
        byte[] columnBytes = BufferUtils.toBytes(columns);
        byte[] valueBytes = BufferUtils.toBytes(values);

        List<byte[]> total = new ArrayList<>();
        total.add(G.I.toBytes(query.tables().get(0)));
        total.add(keysBytes);
        total.add(columnBytes);
        total.add(valueBytes);

        byte[] totalBytes = BufferUtils.toBytes(total);
        int length = BYTE_LENGTH + totalBytes.length;

        return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
    }

    private byte[] handleKvInsert(MqlQuery query) {
        byte method = MdtpMethod.KV_SET;

        List<byte[]> pairs = query.rows().stream()
                .flatMap(Collection::stream)
                .map(G.I::toBytes)
                .toList();

        List<byte[]> total = new ArrayList<>();
        total.add(G.I.toBytes(query.kvstores().get(0)));
        total.addAll(pairs);

        byte[] totalBytes = BufferUtils.toBytes(total);
        int length = BYTE_LENGTH + totalBytes.length;

        return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
    }

    private byte[] handleSelect(MqlQuery query) {
        switch (query.from()) {
            case TABLE -> {
                byte method = MdtpMethod.TABLE_SELECT;
                List<byte[]> total = new ArrayList<>();

                byte[] table = G.I.toBytes(query.tables().get(0));
                List<byte[]> keysList = query.keys().stream().map(G.I::toBytes).toList();
                List<byte[]> columnsList = query.columns().stream().map(G.I::toBytes).toList();

                total.add(table);
                total.add(BufferUtils.toBytes(keysList));
                total.add(BufferUtils.toBytes(columnsList));

                byte[] totalBytes = BufferUtils.toBytes(total);
                int length = BYTE_LENGTH + totalBytes.length;

                return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
            }

            case KVSTORE -> {
                byte method = MdtpMethod.KV_SELECT;
                List<byte[]> total = new ArrayList<>();

                byte[] kvstore = G.I.toBytes(query.kvstores().get(0));
                List<byte[]> keysList = query.keys().stream().map(G.I::toBytes).toList();

                total.add(kvstore);
                total.addAll(keysList);

                byte[] totalBytes = BufferUtils.toBytes(total);
                int length = BYTE_LENGTH + totalBytes.length;

                return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
            }

            default -> throw new UnsupportedOperationException(query.from());
        }
    }

    private byte[] handleShow(MqlQuery query) {
        switch (query.type().toLowerCase()) {
            case KVSTORES -> {
                return new byte[]{MdtpMethod.SHOW_KVSTORE};
            }
            case TABLES -> {
                return new byte[]{MdtpMethod.SHOW_TABLE};
            }
            case COLUMNS -> {
                byte method = MdtpMethod.SHOW_COLUMN;
                byte[] table = G.I.toBytes(query.tables().get(0));

                int length = BYTE_LENGTH + table.length;
                return ByteBuffer.allocate(length).put(method).put(table).array();
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private byte[] handleDelete(MqlQuery query) {
        byte method = TABLE.equalsIgnoreCase(query.from())
                ? MdtpMethod.TABLE_DELETE
                : MdtpMethod.KV_DELETE;

        byte[] name = TABLE.equalsIgnoreCase(query.from())
                ? G.I.toBytes(query.tables().get(0))
                : G.I.toBytes(query.kvstores().get(0));

        List<byte[]> keys = query.keys().stream().map(G.I::toBytes).toList();

        List<byte[]> total = new ArrayList<>();
        total.add(name);
        total.addAll(keys);

        byte[] totalBytes = BufferUtils.toBytes(total);
        int length = BYTE_LENGTH + totalBytes.length;

        return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
    }

    private byte[] handleCreate(MqlQuery query) {
        switch (query.type()) {
            case KVSTORE -> {
                byte method = MdtpMethod.CREATE_KV_STORE;
                List<byte[]> kvstores = query.kvstores().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * kvstores.size());
                kvstores.forEach(kvstore -> length.getAndAdd(kvstore.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(kvstores));

                return buffer.array();
            }

            case TABLE -> {
                byte method = MdtpMethod.CREATE_TABLE;
                List<byte[]> tables = query.tables().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * tables.size());
                tables.forEach(table -> length.getAndAdd(table.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(tables));

                return buffer.array();
            }

            case COLUMNS -> {
                byte method = MdtpMethod.CREATE_TABLE_COLUMN;

                byte[] table = G.I.toBytes(query.tables().get(0));
                List<byte[]> columns = query.columns().stream().map(G.I::toBytes).toList();

                List<byte[]> total = new ArrayList<>();
                total.add(table);
                total.addAll(columns);

                byte[] totalBytes = BufferUtils.toBytes(total);
                int length = BYTE_LENGTH + totalBytes.length;

                return ByteBuffer.allocate(length).put(method).put(totalBytes).array();
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
