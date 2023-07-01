package com.bailizhang.lynxdb.client.connection;

import com.bailizhang.lynxdb.client.annotation.LynxDbColumn;
import com.bailizhang.lynxdb.client.annotation.LynxDbColumnFamily;
import com.bailizhang.lynxdb.client.annotation.LynxDbKey;
import com.bailizhang.lynxdb.client.annotation.LynxDbMainColumn;
import com.bailizhang.lynxdb.core.common.*;
import com.bailizhang.lynxdb.core.health.RecordOption;
import com.bailizhang.lynxdb.core.health.RecordUnit;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FieldUtils;
import com.bailizhang.lynxdb.core.utils.ReflectionUtils;
import com.bailizhang.lynxdb.core.utils.SocketUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.client.SocketClient;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpCode.*;
import static com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod.*;
import static com.bailizhang.lynxdb.ldtp.request.KeyRegister.DEREGISTER;
import static com.bailizhang.lynxdb.ldtp.request.KeyRegister.REGISTER;
import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.JOIN_CLUSTER;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.*;
import static com.bailizhang.lynxdb.ldtp.result.RaftRpcResult.JOIN_CLUSTER_RESULT;


@CheckThreadSafety
public class LynxDbConnection {
    private final ServerNode serverNode;

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

    public synchronized void connect() throws ConnectException {
        if(SocketUtils.isValid(selectionKey)) {
            return;
        }

        try {
            LynxDbFuture<SelectionKey> future = socketClient.connect(serverNode);
            selectionKey = future.get();
        } catch (IOException | CancellationException e) {
            throw new ConnectException("Failed to connect LynxDB server, address: " + serverNode);
        }
    }

    public ServerNode serverNode() {
        return serverNode;
    }

    public SelectionKey selectionKey() throws ConnectException {
        if(SocketUtils.isInvalid(selectionKey)) {
            connect();
        }

        return selectionKey;
    }

    public void disconnect() {
        socketClient.disconnect(selectionKey);
    }

    public byte[] find(byte[] key, String columnFamily, String column) throws ConnectException {
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

    public HashMap<String, byte[]> findMultiColumns(byte[] key, String columnFamily, String... findColumns) throws ConnectException {
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

    public <T> T find(String key, Class<T> type, String... columns) throws ConnectException {
        T obj = ReflectionUtils.newObj(type);

        String columnFamily = findColumnFamily(type);
        Field keyField = findKeyField(type);

        List<Field> columnFields = findColumnFields(type);
        String[] findColumns;

        if(columns.length == 0) {
            findColumns = columnFields.stream().map(Field::getName).toArray(String[]::new);
        } else {
            findColumns = columns;
        }

        HashMap<String, byte[]> multiColumns = findMultiColumns(G.I.toBytes(key), columnFamily, findColumns);

        String mainColumn = findMainColumn(type);
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

    public void insert(byte[] key, String columnFamily, String column, byte[] value) throws ConnectException {
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

    public void insert(byte[] key, byte[] columnFamily, HashMap<String, byte[]> multiColumns) throws ConnectException {
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

    public void insert(Object obj, String... columns) throws ConnectException {
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

    public void delete(byte[] key, String columnFamily, String column) throws ConnectException {
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

    public void deleteMultiColumns(byte[] key, String columnFamily, String... deleteColumns) throws ConnectException {
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

    public void delete(Object obj, String... deleteColumns) throws ConnectException {
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

    public void register(byte[] key, String columnFamily) throws ConnectException {
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

    public void deregister(byte[] key, String columnFamily) throws ConnectException {
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
    ) throws ConnectException {
        return range(
                columnFamily,
                mainColumn,
                beginKey,
                limit,
                RANGE_NEXT,
                findColumns
        );
    }

    public <T> List<T> rangeNext(
            Class<T> clazz,
            byte[] beginKey,
            int limit,
            String... findColumns
    ) throws ConnectException {
        return range(
                clazz,
                beginKey,
                limit,
                this::rangeNext,
                findColumns
        );
    }

    public List<Pair<byte[], HashMap<String, byte[]>>> rangeBefore(
            String columnFamily,
            String mainColumn,
            byte[] endKey,
            int limit,
            String... findColumns
    ) throws ConnectException {
        return range(
                columnFamily,
                mainColumn,
                endKey,
                limit,
                RANGE_BEFORE,
                findColumns
        );
    }

    public <T> List<T> rangeBefore(
            Class<T> clazz,
            byte[] endKey,
            int limit,
            String... findColumns
    ) throws ConnectException {
        return range(
                clazz,
                endKey,
                limit,
                this::rangeBefore,
                findColumns
        );
    }

    public boolean existKey(
            byte[] key,
            String columnFamily,
            String mainColumn
    ) throws ConnectException {
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
    ) throws ConnectException {
        Class<?> clazz = obj.getClass();

        Field keyField = findKeyField(clazz);
        String columnFamily = findColumnFamily(clazz);
        String mainColumn = findMainColumn(clazz);

        byte[] key = G.I.toBytes((String) FieldUtils.get(obj, keyField));

        return existKey(key, columnFamily, mainColumn);
    }

    public void join(String node) throws ConnectException {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(RAFT_RPC);
        bytesList.appendRawByte(JOIN_CLUSTER);
        bytesList.appendRawStr(node);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte type = buffer.get();
        byte success = buffer.get();

        if(type != JOIN_CLUSTER_RESULT || success != TRUE) {
            throw new RuntimeException();
        }
    }

    public List<Pair<RecordOption, Long>> flightRecorder() throws ConnectException {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(FLIGHT_RECORDER);

        SelectionKey selectionKey = selectionKey();
        int serial = socketClient.send(selectionKey, bytesList.toBytes());

        LynxDbFuture<byte[]> future = futureMapGet(selectionKey, serial);
        byte[] data = future.get();

        List<Pair<RecordOption, Long>> recordData = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        while (BufferUtils.isNotOver(buffer)) {
            String name = BufferUtils.getString(buffer);
            byte flag = buffer.get();
            long value = buffer.getLong();

            RecordOption option = new RecordOption(name, RecordUnit.find(flag));
            recordData.add(new Pair<>(option, value));
        }

        return recordData;
    }

    @Override
    public String toString() {
        return serverNode.toString();
    }

    private List<Pair<byte[], HashMap<String, byte[]>>> range(
            String columnFamily,
            String mainColumn,
            byte[] baseKey,
            int limit,
            byte method,
            String... findColumns
    ) throws ConnectException {
        BytesList bytesList = new BytesList(false);
        bytesList.appendRawByte(LDTP_METHOD);
        bytesList.appendRawByte(method);
        bytesList.appendVarStr(columnFamily);
        bytesList.appendVarStr(mainColumn);
        bytesList.appendVarBytes(baseKey);
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

    private  <T> List<T> range(
            Class<T> clazz,
            byte[] baseKey,
            int limit,
            RangeOperator operator,
            String... findColumns
    ) throws ConnectException {
        List<T> objs = new ArrayList<>();

        Field field = findKeyField(clazz);
        String columnFamily = findColumnFamily(clazz);
        String mainColumn = findMainColumn(clazz);

        List<Field> columnFields = findColumnFields(clazz);

        if(findColumns.length == 0) {
            findColumns = columnFields.stream().map(Field::getName).toArray(String[]::new);
        }

        var multiKeys = operator.doRange(columnFamily, mainColumn, baseKey, limit, findColumns);

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

    @FunctionalInterface
    private interface RangeOperator {
        List<Pair<byte[], HashMap<String, byte[]>>> doRange(
                String columnFamily,
                String mainColumn,
                byte[] baseKey,
                int limit,
                String... findColumns
        ) throws ConnectException;
    }
}
