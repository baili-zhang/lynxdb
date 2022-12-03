package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.executor.Executable;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.annotations.LdtpCode;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.bailizhang.lynxdb.socket.code.Request.CLIENT_REQUEST;

public class LynxDbClient {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture>> futureMap = new ConcurrentHashMap<>();

    private final LynxDbConnection connection;
    private final SocketClient socketClient;

    public LynxDbClient() {
        ClientHandler handler = new ClientHandler(futureMap);

        socketClient = new SocketClient();
        socketClient.setHandler(handler);

        connection = new LynxDbConnection(socketClient);
    }

    public void start() {
        Executor.start(socketClient);
    }

    public void connect(String host, int port) throws IOException {
        ServerNode node = new ServerNode(host, port);
        connection.serverNode(node);
    }

    public SelectionKey current() {
        return connection.current();
    }

    public byte[] find(byte[] key, byte[] columnFamily, byte[] column) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.FIND_BY_KEY_CF_COLUMN);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);
        bytesList.appendVarBytes(column);

        SelectionKey current = connection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        return switch (buffer.get()) {
            case LdtpCode.BYTE_ARRAY -> BufferUtils.getRemaining(buffer);
            case LdtpCode.NULL -> null;
            default -> throw new RuntimeException();
        };
    }

    public List<DbValue> find(byte[] key, byte[] columnFamily) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.FIND_BY_KEY_CF);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        SelectionKey current = connection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.DB_VALUE_LIST) {
            throw new RuntimeException();
        }

        List<DbValue> dbValues = new ArrayList<>();
        while (BufferUtils.isNotOver(buffer)) {
            dbValues.add(DbValue.from(buffer));
        }

        return dbValues;
    }

    public void insert(byte[] key, byte[] columnFamily, byte[] column, byte[] value) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.INSERT);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);
        bytesList.appendVarBytes(column);
        bytesList.appendVarBytes(value);

        SelectionKey current = connection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void delete(byte[] key, byte[] columnFamily, byte[] column) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.DELETE);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);
        bytesList.appendVarBytes(column);

        SelectionKey current = connection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    private LynxDbFuture futureMapGet(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture> map = futureMap.get(selectionKey);
        return map.get(serial);
    }
}
