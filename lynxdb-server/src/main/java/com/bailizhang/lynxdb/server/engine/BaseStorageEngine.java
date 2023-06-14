package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.health.FlightDataRecorder;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.lsmtree.LynxDbLsmTree;
import com.bailizhang.lynxdb.lsmtree.Table;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutValue;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;


public class BaseStorageEngine {
    private static final int DEFAULT_MEM_TABLE_SIZE = 4000;
    private static final String TIMEOUT_COLUMN = "timeout";

    protected final Table dataTable;
    protected final Table timeoutTable;

    protected final HashMap<Byte, Method> methodMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public BaseStorageEngine(Class<? extends BaseStorageEngine> clazz) {
        Configuration config = Configuration.getInstance();
        String dataDir = config.dataDir();
        String timeoutDir = config.timeoutDir();

        LsmTreeOptions dataLsmTreeOptions = new LsmTreeOptions(dataDir, DEFAULT_MEM_TABLE_SIZE);
        LsmTreeOptions timeoutLsmTreeOptions = new LsmTreeOptions(timeoutDir, DEFAULT_MEM_TABLE_SIZE);

        dataTable = new LynxDbLsmTree(dataLsmTreeOptions);
        timeoutTable = new LynxDbLsmTree(timeoutLsmTreeOptions);

        initMethod(clazz);
    }

    public synchronized QueryResult doQuery(QueryParams params) {
        Method doQueryMethod = methodMap.get(params.method());
        if(doQueryMethod == null) {
            throw new RuntimeException("Not supported ldtp method.");
        }

        FlightDataRecorder recorder = FlightDataRecorder.recorder();
        recorder.count(FlightDataRecorder.ENGINE_QUERY_COUNT);

        try {
            if(recorder.isEnable()) {
                return (QueryResult) recorder.record(
                        (Callable<Object>) () -> doQueryMethod.invoke(this, params),
                        FlightDataRecorder.ENGINE_DO_QUERY_TIME
                );
            } else {
                return (QueryResult) doQueryMethod.invoke(this, params);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public HashMap<String, byte[]> findAffectKey(MessageKey messageKey) {
        return dataTable.findMultiColumns(messageKey.key(), messageKey.columnFamily());
    }

    public byte[] findTimeoutValue(MessageKey messageKey) {
        return timeoutTable.find(
                messageKey.key(),
                messageKey.columnFamily(),
                TIMEOUT_COLUMN
        );
    }

    public void insertTimeoutKey(TimeoutValue timeoutValue) {
        MessageKey messageKey = timeoutValue.messageKey();

        timeoutTable.insert(
                messageKey.key(),
                messageKey.columnFamily(),
                TIMEOUT_COLUMN,
                timeoutValue.value()
        );
    }

    public void removeTimeoutKey(MessageKey messageKey) {
        timeoutTable.delete(
                messageKey.key(),
                messageKey.columnFamily(),
                TIMEOUT_COLUMN
        );
    }

    public void removeData(MessageKey messageKey) {
        dataTable.deleteMultiColumns(
                messageKey.key(),
                messageKey.columnFamily()
        );
    }

    private void initMethod(Class<? extends BaseStorageEngine> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        Arrays.stream(methods).forEach(method -> {
            LdtpMethod ldtpMethod = method.getAnnotation(LdtpMethod.class);

            if(ldtpMethod == null) {
                return;
            }

            methodMap.put(ldtpMethod.value(), method);
        });
    }
}
