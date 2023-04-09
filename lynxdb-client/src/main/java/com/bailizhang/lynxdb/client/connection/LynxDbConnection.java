package com.bailizhang.lynxdb.client.connection;

import com.bailizhang.lynxdb.client.annotation.LynxDbColumn;
import com.bailizhang.lynxdb.client.annotation.LynxDbColumnFamily;
import com.bailizhang.lynxdb.client.annotation.LynxDbKey;
import com.bailizhang.lynxdb.client.annotation.LynxDbMainColumn;
import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.common.LynxDbFuture;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.core.utils.ReflectionUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.*;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod.*;
import static com.bailizhang.lynxdb.ldtp.request.KeyRegister.DEREGISTER;
import static com.bailizhang.lynxdb.ldtp.request.KeyRegister.REGISTER;
import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.JOIN_CLUSTER;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.*;

/**
 * TODO: 2000 行以后再分成多个类
 */
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

    public byte[] find(byte[] key, String columnFamily, String column) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(LdtpMethod.FIND_BY_KEY_CF_COLUMN);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);
        bytesList.appendVarStr(column);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        return switch (buffer.get()) {
            case BYTE_ARRAY -> BufferUtils.getRemaining(buffer);
            case NULL -> null;
            default -> throw new RuntimeException();
        };
    }

    public HashMap<String, byte[]> findMultiColumns(byte[] key, String columnFamily, String... findColumns) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(FIND_MULTI_COLUMNS);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);

        for(String findColumn : findColumns) {
            bytesList.appendVarStr(findColumn);
        }

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte flag = buffer.get();
        if (flag != MULTI_COLUMNS) {
            throw new RuntimeException();
        }

        HashMap<String, byte[]> multiColumns = new HashMap<>();
        while (BufferUtils.isNotOver(buffer)) {
            String column = BufferUtils.getString(buffer);
            byte valueFlag = buffer.get();

            byte[] value = switch (valueFlag) {
                case BYTE_ARRAY -> BufferUtils.getBytes(buffer);
                case NULL -> null;
                default -> throw new RuntimeException();
            };

            multiColumns.put(column, value);
        }

        return multiColumns;
    }

    @SuppressWarnings("unchecked")
    public <T> T find(T findObj, String... columns) {
        Class<T> clazz = (Class<T>) findObj.getClass();
        T obj = ReflectionUtils.newObj(clazz);

        String columnFamily = findColumnFamily(clazz);
        Field keyField = findKeyField(clazz);

        List<Field> columnFields = findColumnFields(clazz);
        String[] findColumns;

        if(columns.length == 0) {
            findColumns = columnFields.stream().map(Field::getName).toArray(String[]::new);
        } else {
            findColumns = columns;
        }

        String key = (String) FieldUtils.get(findObj, keyField);

        HashMap<String, byte[]> multiColumns = findMultiColumns(G.I.toBytes(key), columnFamily, findColumns);

        String mainColumn = findMainColumn(clazz);
        if(multiColumns.get(mainColumn) == null) {
            return null;
        }

        FieldUtils.set(obj, keyField, key);

        multiColumns.forEach((column, value) -> {
            String val = G.I.toString(value);

            // TODO: 支持 String 以外的其他类型
            FieldUtils.set(obj, column, val);
        });

        return obj;
    }

    public void insert(byte[] key, String columnFamily, String column, byte[] value) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(LdtpMethod.INSERT);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);
        bytesList.appendVarStr(column);
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

    public void insert(byte[] key, byte[] columnFamily, HashMap<String, byte[]> multiColumns) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(LdtpMethod.INSERT_MULTI_COLUMNS);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(columnFamily);

        multiColumns.forEach((column, value) -> {
            bytesList.appendVarStr(column);
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

    public void insert(Object obj, String... columns) {
        Class<?> clazz = obj.getClass();

        String columnFamily = findColumnFamily(clazz);
        Field keyField = findKeyField(clazz);
        List<Field> columnFields = findColumnFields(clazz);

        List<Field> insertColumnFields;

        if(columns.length == 0) {
            insertColumnFields = columnFields;
        } else {
            HashSet<String> insertColumnSet = new HashSet<>(Arrays.asList(columns));
            insertColumnFields = columnFields.stream()
                    .filter(field -> insertColumnSet.contains(field.getName()))
                    .toList();
        }

        String key = (String) FieldUtils.get(obj, keyField);

        HashMap<String, byte[]> multiColumns = new HashMap<>();
        for(Field field : insertColumnFields) {
            String column = field.getName();
            String value = (String) FieldUtils.get(obj, field);

            multiColumns.put(column, G.I.toBytes(value));
        }

        insert(G.I.toBytes(key), G.I.toBytes(columnFamily), multiColumns);
    }

    public void delete(byte[] key, String columnFamily, String column) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(DELETE);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);
        bytesList.appendVarStr(column);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void deleteMultiColumns(byte[] key, String columnFamily, String... deleteColumns) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(DELETE_MULTI_COLUMNS);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);

        for(String deleteColumn : deleteColumns) {
            bytesList.appendVarStr(deleteColumn);
        }

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void delete(Object obj, String... deleteColumns) {
        Class<?> clazz = obj.getClass();

        Field keyField = findKeyField(clazz);
        String columnFamily = findColumnFamily(clazz);
        List<Field> columnFields = findColumnFields(clazz);

        if(deleteColumns == null || deleteColumns.length == 0) {
             deleteColumns = columnFields.stream().map(Field::getName).toArray(String[]::new);
        }

        byte[] key = G.I.toBytes((String) FieldUtils.get(obj, keyField));

        deleteMultiColumns(key, columnFamily, deleteColumns);
    }

    public void register(byte[] key, String columnFamily) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(KEY_REGISTER);
        bytesList.appendRawByte(REGISTER);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public void deregister(byte[] key, String columnFamily) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(KEY_REGISTER);
        bytesList.appendRawByte(DEREGISTER);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        if (buffer.get() != LdtpCode.VOID) {
            throw new RuntimeException();
        }
    }

    public List<Pair<byte[], HashMap<String, byte[]>>> rangeNext(
            String columnFamily,
            String mainColumn,
            byte[] beginKey,
            int limit,
            String... findColumns
    ) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(LdtpMethod.RANGE_NEXT);
        bytesList.appendVarStr(columnFamily);
        bytesList.appendVarStr(mainColumn);
        bytesList.appendVarBytes(beginKey);
        bytesList.appendRawInt(limit);

        for(String findColumn : findColumns) {
            bytesList.appendVarStr(findColumn);
        }

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        if (buffer.get() != LdtpCode.MULTI_KEYS) {
            throw new RuntimeException();
        }

        List<Pair<byte[], HashMap<String, byte[]>>> multiKeys = new ArrayList<>();

        while(BufferUtils.isNotOver(buffer)) {
            byte[] key = BufferUtils.getBytes(buffer);
            int size = buffer.getInt();

            HashMap<String, byte[]> multiColumns = new HashMap<>();
            multiKeys.add(new Pair<>(key, multiColumns));

            while((size --) > 0) {
                String column = BufferUtils.getString(buffer);
                byte flag = buffer.get();

                byte[] value = switch (flag) {
                    case BYTE_ARRAY -> BufferUtils.getBytes(buffer);
                    case NULL -> null;
                    default -> throw new RuntimeException();
                };
                multiColumns.put(column, value);
            }
        }

        return multiKeys;
    }

    public <T> List<T> rangeNext(Class<T> clazz, byte[] beginKey, int limit, String... findColumns) {
        List<T> objs = new ArrayList<>();

        Field field = findKeyField(clazz);
        String columnFamily = findColumnFamily(clazz);
        String mainColumn = findMainColumn(clazz);

        List<Field> columnFields = findColumnFields(clazz);

        if(findColumns.length == 0) {
            findColumns = columnFields.stream().map(Field::getName).toArray(String[]::new);
        }

        var multiKeys = rangeNext(columnFamily, mainColumn, beginKey, limit, findColumns);

        multiKeys.forEach(pair -> {
            byte[] key = pair.left();
            var multiColumns = pair.right();

            T obj = ReflectionUtils.newObj(clazz);

            FieldUtils.set(obj, field, G.I.toString(key));

            multiColumns.forEach((column, value) -> {
                FieldUtils.set(obj, column, G.I.toString(value));
            });

            objs.add(obj);
        });

        return objs;
    }

    public boolean existKey(
            byte[] key,
            String columnFamily,
            String mainColumn
    ) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(LdtpMethod.EXIST_KEY);
        bytesList.appendVarBytes(key);
        bytesList.appendVarStr(columnFamily);
        bytesList.appendVarStr(mainColumn);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        return switch (buffer.get()) {
            case TRUE -> true;
            case FALSE -> false;
            default -> throw new RuntimeException();
        };
    }

    public boolean existKey(
            Object obj
    ) {
        Class<?> clazz = obj.getClass();

        Field keyField = findKeyField(clazz);
        String columnFamily = findColumnFamily(clazz);
        String mainColumn = findMainColumn(clazz);

        byte[] key = G.I.toBytes((String) FieldUtils.get(obj, keyField));

        return existKey(key, columnFamily, mainColumn);
    }

    public void join(String node) {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(RAFT_RPC);
        bytesList.appendRawByte(JOIN_CLUSTER);
        bytesList.appendRawStr(node);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        // todo
    }

    @Override
    public String toString() {
        return serverNode.toString();
    }

    private LynxDbFuture<byte[]> futureMapGet(SelectionKey selectionKey, int serial) {
        ConcurrentHashMap<Integer, LynxDbFuture<byte[]>> map = futureMap.get(selectionKey);
        return map.get(serial);
    }

    private String findColumnFamily(Class<?> clazz) {
        LynxDbColumnFamily annotation = clazz.getAnnotation(LynxDbColumnFamily.class);
        if(annotation == null) {
            throw new RuntimeException();
        }

        String columnFamily = annotation.value();
        if(columnFamily == null) {
            throw new RuntimeException();
        }

        return columnFamily;
    }

    private String findMainColumn(Class<?> clazz) {
        // TODO: class 的 field 可以缓存，是否可以提高性能？
        Field[] fields = clazz.getDeclaredFields();
        List<Field> mainColumnFields = new ArrayList<>();

        for (Field field : fields) {
            if(FieldUtils.isAnnotated(field, LynxDbMainColumn.class)
                    && FieldUtils.isAnnotated(field, LynxDbColumn.class)
            ) {
                mainColumnFields.add(field);
            }
        }

        if(mainColumnFields.size() != 1) {
            throw new RuntimeException();
        }

        Field mainColumnField = mainColumnFields.get(0);
        Class<?> mainColumnClazz = mainColumnField.getType();

        if(mainColumnClazz != String.class) {
            throw new RuntimeException();
        }

        return mainColumnField.getName();
    }

    private Field findKeyField(Class<?> clazz) {
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

        return keyField;
    }

    private List<Field> findColumnFields(Class<?> clazz) {
        // TODO: class 的 field 可以缓存，是否可以提高性能？
        Field[] fields = clazz.getDeclaredFields();
        List<Field> columnFields = new ArrayList<>();

        for (Field field : fields) {
            if(FieldUtils.isAnnotated(field, LynxDbColumn.class)) {
                Class<?> keyClazz = field.getType();
                if(keyClazz != String.class) {
                    throw new RuntimeException();
                }

                columnFields.add(field);
            }
        }

        if(columnFields.isEmpty()) {
            throw new RuntimeException();
        }

        return columnFields;
    }
}
