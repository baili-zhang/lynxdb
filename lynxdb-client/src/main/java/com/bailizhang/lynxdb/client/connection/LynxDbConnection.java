package com.bailizhang.lynxdb.client.connection;

import com.bailizhang.lynxdb.client.annotation.LynxDbColumn;
import com.bailizhang.lynxdb.client.annotation.LynxDbColumnFamily;
import com.bailizhang.lynxdb.client.annotation.LynxDbKey;
import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static com.bailizhang.lynxdb.socket.code.Request.*;
import static com.bailizhang.lynxdb.socket.code.Request.CLIENT_REQUEST;

public class LynxDbConnection {
    private final ServerNode serverNode;

    // TODO 需要检测已经失效的 selectionKey
    private final ConcurrentHashMap<SelectionKey, ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> futureMap;

    private SelectionKey selectionKey;

    protected final SocketClient socketClient;

    public LynxDbConnection(
            ServerNode node,
            SocketClient client,
            ConcurrentHashMap<SelectionKey, ConcurrentHashMap<Integer, LynxDbFuture<byte[]>>> map
    ) {
        serverNode = node;
        socketClient = client;
        futureMap = map;
    }

    public void connect() {
        if(selectionKey != null && selectionKey.isValid()) {
            return;
        }

        try {
            LynxDbFuture<SelectionKey> future = socketClient.connect(serverNode);
            selectionKey = future.get();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public SelectionKey selectionKey() {
        if(selectionKey == null || !selectionKey.isValid()) {
            connect();
        }

        return selectionKey;
    }

    public void disconnect() {
        socketClient.disconnect(selectionKey);
    }

    public byte[] find(byte[] key, byte[] columnFamily, byte[] column) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(CLIENT_REQUEST);
        bytesList.appendRawByte(LdtpMethod.FIND_BY_KEY_CF_COLUMN);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);
        bytesList.appendVarBytes(column);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
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

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    @Override
    public String toString() {
        return serverNode.toString();
    }

    private LynxDbFuture<byte[]> futureMapGet(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(selectionKey);
        return map.get(serial);
    }
}
