package com.bailizhang.lynxdb.client;

import com.bailizhang.lynxdb.client.annotation.LynxDbColumn;
import com.bailizhang.lynxdb.client.annotation.LynxDbColumnFamily;
import com.bailizhang.lynxdb.client.annotation.LynxDbKey;
import com.bailizhang.lynxdb.client.message.MessageHandler;
import com.bailizhang.lynxdb.client.message.MessageReceiver;
import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.executor.Executor;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.annotations.LdtpCode;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.message.MessageKey;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;
import com.bailizhang.lynxdb.socket.request.SocketRequest;

import java.lang.reflect.Field;
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

    private final LynxDbConnection connection;
    private final LynxDbConnection registerConnection;
    private final SocketClient socketClient;

    private final MessageReceiver messageReceiver;

    public LynxDbClient() {
        messageReceiver = new MessageReceiver();

        ClientHandler handler = new ClientHandler(futureMap, messageReceiver);

        socketClient = new SocketClient();
        socketClient.setHandler(handler);

        connection = new LynxDbConnection(socketClient);
        registerConnection = new LynxDbConnection(socketClient);
    }

    public void start() {
        Executor.start(socketClient);
        Executor.start(messageReceiver);
    }

    public void connect(String host, int port) {
        // 创建请求端口的连接
        ServerNode node = new ServerNode(host, port);
        connection.serverNode(node);
    }

    public void disconnect() {
        socketClient.send(selectionKey(), SocketRequest.BLANK_FLAG, BufferUtils.EMPTY_BYTES);
    }

    public void registerConnect(String host, int port) {
        // 创建注册监听 key 端口的连接
        ServerNode node = new ServerNode(host, port);
        registerConnection.serverNode(node);
    }

    public SelectionKey selectionKey() {
        return connection.current();
    }

    public SelectionKey messageSelectionKey() {
        return registerConnection.current();
    }

    public void registerAffectHandler(MessageKey messageKey, MessageHandler messageHandler) {
        messageReceiver.registerAffectHandler(messageKey, messageHandler);
    }

    public void registerTimeoutHandler(MessageKey messageKey, MessageHandler messageHandler) {
        messageReceiver.registerTimeoutHandler(messageKey, messageHandler);
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

    public <T> T find(T obj, byte[]... columns) {
        Class<?> clazz = obj.getClass();
        LynxDbColumnFamily annotation = clazz.getAnnotation(LynxDbColumnFamily.class);
        if(annotation == null) {
            throw new RuntimeException();
        }

        String columnFamily = annotation.value();
        if(columnFamily == null) {
            throw new RuntimeException();
        }

        // TODO: class 的 field 可以缓存，是否可以提高性能？
        Field[] fields = clazz.getDeclaredFields();
        List<Field> keyFields = new ArrayList<>();

        for (Field field : fields) {
            if(FieldUtils.isAnnotated(field, LynxDbKey.class)) {
                keyFields.add(field);
            }
        }

        if(keyFields.size() != 1) {
            throw new RuntimeException();
        }

        Field keyField = keyFields.get(0);
        Class<?> keyClazz = keyField.getType();

        if(keyClazz != String.class) {
            throw new RuntimeException();
        }

        String key = (String) FieldUtils.get(obj, keyField);
        if(columns.length != 0) {
            // TODO: 支持选择 column
            throw new RuntimeException();
        }

        List<DbValue> dbValues = find(G.I.toBytes(key), G.I.toBytes(columnFamily));
        dbValues.forEach(dbValue -> {
            String name = G.I.toString(dbValue.column());
            String value = G.I.toString(dbValue.value());

            // TODO: 支持 String 以外的其他类型
            FieldUtils.set(obj, name, value);
        });

        return obj;
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

    public void insert(byte[] key, byte[] columnFamily, List<DbValue> dbValues) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.INSERT_MULTI_COLUMN);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        dbValues.forEach(dbValue -> {
            byte[] column = dbValue.column();
            byte[] value = dbValue.value();

            bytesList.appendVarBytes(column);
            bytesList.appendVarBytes(value);
        });

        SelectionKey current = connection.current();
        int serial = socketClient.send(current, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(current, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void insert(Object obj, byte[]... columns) {
        Class<?> clazz = obj.getClass();
        LynxDbColumnFamily annotation = clazz.getAnnotation(LynxDbColumnFamily.class);
        if(annotation == null) {
            throw new RuntimeException();
        }

        String columnFamily = annotation.value();
        if(columnFamily == null) {
            throw new RuntimeException();
        }

        // TODO: class 的 field 可以缓存，是否可以提高性能？
        Field[] fields = clazz.getDeclaredFields();
        List<Field> keyFields = new ArrayList<>();

        for (Field field : fields) {
            if(FieldUtils.isAnnotated(field, LynxDbKey.class)) {
                keyFields.add(field);
            }
        }

        if(keyFields.size() != 1) {
            throw new RuntimeException();
        }

        Field keyField = keyFields.get(0);
        Class<?> keyClazz = keyField.getType();

        if(keyClazz != String.class) {
            throw new RuntimeException();
        }

        String key = (String) FieldUtils.get(obj, keyField);
        if(columns.length != 0) {
            // TODO: 支持选择 column
            throw new RuntimeException();
        }

        List<DbValue> dbValues = new ArrayList<>();
        for(Field field : fields) {
            if(field.getType() != String.class) {
                continue;
            }

            if(!FieldUtils.isAnnotated(field, LynxDbColumn.class)) {
                continue;
            }

            String column = field.getName();
            String value = (String) FieldUtils.get(obj, field);

            DbValue dbValue = new DbValue(G.I.toBytes(column), G.I.toBytes(value));
            dbValues.add(dbValue);
        }

        insert(G.I.toBytes(key), G.I.toBytes(columnFamily), dbValues);
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

    public void delete(byte[] key, byte[] columnFamily) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.DELETE);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

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

    private LynxDbFuture<byte[]> futureMapGet(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(selectionKey);
        return map.get(serial);
    }

    @Override
    public void close() {
        socketClient.close();
    }
}
