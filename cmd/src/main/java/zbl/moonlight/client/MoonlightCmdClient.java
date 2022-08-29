package zbl.moonlight.client;

import zbl.moonlight.client.mql.MQL;
import zbl.moonlight.client.mql.MqlQuery;
import zbl.moonlight.client.printer.Printer;
import zbl.moonlight.core.common.Converter;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.executor.Shutdown;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import static zbl.moonlight.client.mql.MQL.Keywords.*;
import static zbl.moonlight.core.utils.NumberUtils.BYTE_LENGTH;
import static zbl.moonlight.core.utils.NumberUtils.INT_LENGTH;

public class MoonlightCmdClient extends Shutdown {
    private final AsyncMoonlightClient client = new AsyncMoonlightClient();
    private final Scanner scanner = new Scanner(System.in);

    private final AtomicInteger serial = new AtomicInteger(1);

    /**
     * 终端当前连接的节点
     */
    private volatile SelectionKey current;

    public MoonlightCmdClient() {
        G.I.converter(new Converter(StandardCharsets.UTF_8));
    }

    public void start() throws IOException {
        Executor.start(client);
        ServerNode serverNode = new ServerNode("127.0.0.1", 7820);
        current = client.connect(serverNode);

        while (isNotShutdown()) {
            Printer.printPrompt(current);

            StringBuilder temp = new StringBuilder();
            String line;

            while (!(line = scanner.nextLine()).trim().endsWith(";")) {
                temp.append(" ").append(line);
            }

            temp.append(" ").append(line);
            String statement = temp.toString();

            List<MqlQuery> queries = MQL.parse(statement);

            for(MqlQuery query : queries) {
                switch (query.name()) {
                    case CREATE -> handleCreate(query);
                    case DROP -> handleDrop(query);
                    case DELETE -> handleDelete(query);
                    case SHOW -> handleShow(query);
                    case SELECT -> handleSelect(query);
                    case INSERT -> {
                        if(TABLE.equalsIgnoreCase(query.type())) {
                            handleTableInsert(query);
                        } else {
                            handleKvInsert(query);
                        }
                    }

                    default -> throw new UnsupportedOperationException(query.name());
                }
            }
        }
    }

    private void handleDrop(MqlQuery query) {
        switch (query.type()) {
            case KVSTORE -> {
                MoonlightFuture future = client.asyncCreateKvstore(current, query.kvstores());
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case TABLE -> {
                MoonlightFuture future = client.asyncCreateTable(current, query.tables());
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case COLUMNS -> {
                String table = query.tables().get(0);
                List<byte[]> columns = query.columns().stream().map(G.I::toBytes).toList();
                MoonlightFuture future = client.asyncCreateTableColumn(current, table, columns);
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private void handleTableInsert(MqlQuery query) {
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

        ByteBuffer.allocate(length).put(method).put(totalBytes);
    }

    private void handleKvInsert(MqlQuery query) {
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

        ByteBuffer.allocate(length).put(method).put(totalBytes);
    }

    private void handleSelect(MqlQuery query) {
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

                ByteBuffer.allocate(length).put(method).put(totalBytes);
            }

            case KVSTORE -> {
                byte method = MdtpMethod.KV_GET;
                List<byte[]> total = new ArrayList<>();

                byte[] kvstore = G.I.toBytes(query.kvstores().get(0));
                List<byte[]> keysList = query.keys().stream().map(G.I::toBytes).toList();

                total.add(kvstore);
                total.addAll(keysList);

                byte[] totalBytes = BufferUtils.toBytes(total);
                int length = BYTE_LENGTH + totalBytes.length;

                ByteBuffer.allocate(length).put(method).put(totalBytes);
            }

            default -> throw new UnsupportedOperationException(query.from());
        }
    }

    private void handleShow(MqlQuery query) {
        switch (query.type().toLowerCase()) {
            case KVSTORES -> {
            }
            case TABLES -> {
            }
            case COLUMNS -> {
                byte method = MdtpMethod.SHOW_COLUMN;
                byte[] table = G.I.toBytes(query.tables().get(0));

                int length = BYTE_LENGTH + table.length;
                ByteBuffer.allocate(length).put(method).put(table);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private void handleDelete(MqlQuery query) {
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

        ByteBuffer.allocate(length).put(method).put(totalBytes);
    }

    private void handleCreate(MqlQuery query) {
        switch (query.type()) {
            case KVSTORE -> {
                byte method = MdtpMethod.CREATE_KV_STORE;
                List<byte[]> kvstores = query.kvstores().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * kvstores.size());
                kvstores.forEach(kvstore -> length.getAndAdd(kvstore.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(kvstores));

            }

            case TABLE -> {
                byte method = MdtpMethod.CREATE_TABLE;
                List<byte[]> tables = query.tables().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * tables.size());
                tables.forEach(table -> length.getAndAdd(table.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(tables));

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

                ByteBuffer.allocate(length).put(method).put(totalBytes);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    public static void main(String[] args) throws IOException {
        MoonlightCmdClient client = new MoonlightCmdClient();
        client.start();
    }

    @Override
    protected void doAfterShutdown() {
        if(current != null) {
            disconnect();
        }

        client.shutdown();
    }

    private void disconnect() {
        Printer.printDisconnect(current);
        current = null;
    }

    public void setCurrent(SelectionKey key) {
        this.current = key;
    }
}
