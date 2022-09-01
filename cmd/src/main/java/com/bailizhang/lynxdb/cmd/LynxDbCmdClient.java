package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.cmd.lql.LQL;
import com.bailizhang.lynxdb.cmd.lql.LqlQuery;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.client.AsyncLynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbFuture;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicInteger;

import static com.bailizhang.lynxdb.cmd.lql.LQL.Keywords.*;
import static com.bailizhang.lynxdb.core.utils.NumberUtils.BYTE_LENGTH;
import static com.bailizhang.lynxdb.core.utils.NumberUtils.INT_LENGTH;

public class LynxDbCmdClient extends Shutdown {
    private final AsyncLynxDbClient client = new AsyncLynxDbClient();
    private final Scanner scanner = new Scanner(System.in);

    private final AtomicInteger serial = new AtomicInteger(1);

    /**
     * 终端当前连接的节点
     */
    private volatile SelectionKey current;

    public LynxDbCmdClient() {
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

            List<LqlQuery> queries = LQL.parse(statement);

            for(LqlQuery query : queries) {
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

    private void handleDrop(LqlQuery query) {
        switch (query.type()) {
            case KVSTORE -> {
                LynxDbFuture future = client.asyncCreateKvstore(current, query.kvstores());
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case TABLE -> {
                LynxDbFuture future = client.asyncCreateTable(current, query.tables());
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case COLUMNS -> {
                String table = query.tables().get(0);
                List<byte[]> columns = query.columns().stream().map(G.I::toBytes).toList();
                LynxDbFuture future = client.asyncCreateTableColumn(current, table, columns);
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private void handleTableInsert(LqlQuery query) {
        byte method = LdtpMethod.TABLE_INSERT;

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

    private void handleKvInsert(LqlQuery query) {
        byte method = LdtpMethod.KV_SET;

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

    private void handleSelect(LqlQuery query) {
        switch (query.from()) {
            case TABLE -> {
                byte method = LdtpMethod.TABLE_SELECT;
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
                String kvstore = query.kvstores().get(0);
                List<byte[]> keys = query.keys().stream().map(G.I::toBytes).toList();
                LynxDbFuture future = client.asyncKvGet(current, kvstore, keys);
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            default -> throw new UnsupportedOperationException(query.from());
        }
    }

    private void handleShow(LqlQuery query) {
        switch (query.type().toLowerCase()) {
            case KVSTORES -> {
            }
            case TABLES -> {
            }
            case COLUMNS -> {
                byte method = LdtpMethod.SHOW_COLUMN;
                byte[] table = G.I.toBytes(query.tables().get(0));

                int length = BYTE_LENGTH + table.length;
                ByteBuffer.allocate(length).put(method).put(table);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private void handleDelete(LqlQuery query) {
        byte method = TABLE.equalsIgnoreCase(query.from())
                ? LdtpMethod.TABLE_DELETE
                : LdtpMethod.KV_DELETE;

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

    private void handleCreate(LqlQuery query) {
        switch (query.type()) {
            case KVSTORE -> {
                byte method = LdtpMethod.CREATE_KV_STORE;
                List<byte[]> kvstores = query.kvstores().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * kvstores.size());
                kvstores.forEach(kvstore -> length.getAndAdd(kvstore.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(kvstores));

            }

            case TABLE -> {
                byte method = LdtpMethod.CREATE_TABLE;
                List<byte[]> tables = query.tables().stream().map(G.I::toBytes).toList();

                AtomicInteger length = new AtomicInteger(BYTE_LENGTH + INT_LENGTH * tables.size());
                tables.forEach(table -> length.getAndAdd(table.length));

                ByteBuffer buffer = ByteBuffer.allocate(length.get());

                buffer.put(method);
                buffer.put(BufferUtils.toBytes(tables));

            }

            case COLUMNS -> {
                byte method = LdtpMethod.CREATE_TABLE_COLUMN;

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
        LynxDbCmdClient client = new LynxDbCmdClient();
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
}
