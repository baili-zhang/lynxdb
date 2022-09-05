package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.cmd.exception.SyntaxException;
import com.bailizhang.lynxdb.cmd.lql.LQL;
import com.bailizhang.lynxdb.cmd.lql.LqlQuery;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.Converter;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.executor.Shutdown;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.client.AsyncLynxDbClient;
import com.bailizhang.lynxdb.client.LynxDbFuture;
import com.bailizhang.lynxdb.storage.core.*;

import java.io.IOException;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.*;

import static com.bailizhang.lynxdb.cmd.lql.LQL.Keywords.*;

public class LynxDbCmdClient extends Shutdown {
    private static final String TABLES_HEADER = "Tables";
    private static final String KVSTORE_HEADER = "KV Stores";
    private static final String TABLE_COLUMN_HEADER = "Columns";
    private static final String VALUE_COLUMN = "value";

    public static final String KEY_HEADER = "key";

    private final AsyncLynxDbClient client = new AsyncLynxDbClient();
    private final Scanner scanner = new Scanner(System.in);

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

            List<LqlQuery> queries;

            try {
                queries = LQL.parse(statement);
            } catch (SyntaxException e) {
                Printer.printRawMessage(e.getMessage());
                continue;
            }

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
                LynxDbFuture future = client.asyncDropKvstore(current, query.kvstores());
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case TABLE -> {
                LynxDbFuture future = client.asyncDropTable(current, query.tables());
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case COLUMNS -> {
                String table = query.tables().get(0);
                HashSet<Column> columns = new HashSet<>();

                for (String column : query.columns()) {
                    Column c = new Column(G.I.toBytes(column));
                    columns.add(c);
                }

                LynxDbFuture future = client.asyncDropTableColumn(current, table, columns);
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private void handleTableInsert(LqlQuery query) {
        String table = query.tables().get(0);
        List<String> columns = query.columns();
        MultiTableRows keys = new MultiTableRows();

        for(List<String> row : query.rows()) {
            Key key = new Key(G.I.toBytes(row.get(0)));

            Map<Column, byte[]> rowMap = new HashMap<>();
            for(int i = 0; i < columns.size(); i ++) {
                Column column = new Column(G.I.toBytes(columns.get(i)));
                byte[] value = G.I.toBytes(row.get(i + 1));
                rowMap.put(column, value);
            }

            keys.put(key, rowMap);
        }

        LynxDbFuture future = client.asyncTableInsert(current, table, keys);
        byte[] response = future.get();
        Printer.printResponse(response);
    }

    private void handleKvInsert(LqlQuery query) {
        String kvstore = query.kvstores().get(0);
        List<Pair<byte[], byte[]>> kvPairs = new ArrayList<>();

        for(List<String> row : query.rows()) {
            byte[] key = G.I.toBytes(row.get(0));
            byte[] value = G.I.toBytes(row.get(1));

            kvPairs.add(new Pair<>(key, value));
        }

        LynxDbFuture future = client.asyncKvSet(current, kvstore, kvPairs);
        byte[] response = future.get();
        Printer.printResponse(response);
    }

    private void handleSelect(LqlQuery query) {
        switch (query.from()) {
            case TABLE -> {
                List<byte[]> keys = query.keys().stream().map(G.I::toBytes).toList();
                HashSet<Column> columns = new HashSet<>(
                        query.columns().stream()
                                .map(G.I::toBytes)
                                .map(Column::new)
                                .toList()
                );

                MultiTableKeys multiTableKeys = new MultiTableKeys(keys, columns);
                String table = query.tables().get(0);
                LynxDbFuture future = client.asyncTableSelect(current, table, multiTableKeys);

                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case KVSTORE -> {
                String kvstore = query.kvstores().get(0);
                List<byte[]> keys = query.keys().stream().map(G.I::toBytes).toList();
                LynxDbFuture future = client.asyncKvGet(current, kvstore, keys);
                byte[] response = future.get();
                Printer.printKvPairs(response, List.of(KEY_HEADER, VALUE_COLUMN));
            }

            default -> throw new UnsupportedOperationException(query.from());
        }
    }

    private void handleShow(LqlQuery query) {
        switch (query.type().toLowerCase()) {
            case KVSTORES -> {
                LynxDbFuture future = client.asyncShowKvstore(current);
                byte[] response = future.get();
                Printer.printList(response, KVSTORE_HEADER);
            }
            case TABLES -> {
                LynxDbFuture future = client.asyncShowTable(current);
                byte[] response = future.get();
                Printer.printList(response, TABLES_HEADER);
            }
            case COLUMNS -> {
                String table = query.tables().get(0);
                LynxDbFuture future = client.asyncShowTableColumn(current, table);
                byte[] response = future.get();
                Printer.printList(response, TABLE_COLUMN_HEADER);
            }

            default -> throw new UnsupportedOperationException(query.type());
        }
    }

    private void handleDelete(LqlQuery query) {
        switch (query.from()) {
            case TABLE -> {
                String table = query.tables().get(0);
                List<byte[]> keys = query.keys().stream().map(G.I::toBytes).toList();

                LynxDbFuture future = client.asyncTableDelete(current, table, keys);
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            case KVSTORE -> {
                String kvstore = query.kvstores().get(0);
                List<byte[]> keys = query.keys().stream().map(G.I::toBytes).toList();

                LynxDbFuture future = client.asyncKvDelete(current, kvstore, keys);
                byte[] response = future.get();
                Printer.printResponse(response);
            }

            default -> throw new UnsupportedOperationException(query.from());
        }
    }

    private void handleCreate(LqlQuery query) {
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
