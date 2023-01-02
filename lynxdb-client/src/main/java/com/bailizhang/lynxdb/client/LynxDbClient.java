package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.annotations.LdtpCode;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.affect.AffectValue;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import static com.bailizhang.lynxdb.socket.code.Request.*;

public class LynxDbClient implements AutoCloseable {
    private final ConcurrentHashMap<SelectionKey,
            ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> futureMap = new ConcurrentHashMap<>();
    private final BlockingQueue<byte[]> messageQueue = new LinkedBlockingQueue<>();

    private final LynxDbConnection connection;
    private final LynxDbConnection registerConnection;
    private final SocketClient socketClient;

    public LynxDbClient() {
        ClientHandler handler = new ClientHandler(futureMap, messageQueue);

        socketClient = new SocketClient();
        socketClient.setHandler(handler);

        connection = new LynxDbConnection(socketClient);
        registerConnection = new LynxDbConnection(socketClient);
    }

    public void start() {
        Executor.start(socketClient);
    }

    public void connect(String host, int port) {
        // 创建请求端口的连接
        ServerNode node = new ServerNode(host, port);
        connection.serverNode(node);
    }

    public void disconnect() {
        socketClient.send(current(), SocketRequest.BLANK_FLAG, BufferUtils.EMPTY_BYTES);
    }

    public void registerConnect(String host, int port) {
        // 创建注册监听 key 端口的连接
        ServerNode node = new ServerNode(host, port);
        registerConnection.serverNode(node);
    }

    public SelectionKey current() {
        return connection.current();
    }

    public SelectionKey registerCurrent() {
        return registerConnection.current();
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

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
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

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
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

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
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

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void register(byte[] key, byte[] columnFamily) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(REGISTER_KEY);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        SelectionKey current = registerConnection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void deregister(byte[] key, byte[] columnFamily) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(DEREGISTER_KEY);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        SelectionKey current = registerConnection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public AffectValue onMessage() {
        try {
            byte[] data = messageQueue.take();
            ByteBuffer buffer = ByteBuffer.wrap(data);
            return AffectValue.from(buffer);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private LynxDbFuture<byte[]> futureMapGet(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(selectionKey);
        return map.get(serial);
    }

    @Override
    public void close() {
        socketClient.close();
    }
}
