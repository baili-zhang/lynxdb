package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.lsmtree.LsmTree;
import com.bailizhang.lynxdb.lsmtree.LynxDbLsmTree;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutKey;
import com.bailizhang.lynxdb.server.engine.timeout.TimeoutValue;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class BaseStorageEngine {
    private static final int DEFAULT_MEM_TABLE_SIZE = 4000;
    private static final byte[] TIMEOUT_COLUMN = G.I.toBytes("timeout");

    protected final LsmTree dataLsmTree;
    protected final LsmTree timeoutLsmTree;

    protected final HashMap<Byte, Method> methodMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public BaseStorageEngine(Class<? extends BaseStorageEngine> clazz) {
        Configuration config = Configuration.getInstance();
        String dataDir = config.dataDir();
        String timeoutDir = config.timeoutDir();

        Options options = new Options(DEFAULT_MEM_TABLE_SIZE);

        dataLsmTree = new LynxDbLsmTree(dataDir, options);
        timeoutLsmTree = new LynxDbLsmTree(timeoutDir, options);

        initMethod(clazz);
    }

    public synchronized QueryResult doQuery(QueryParams params) {
        Method doQueryMethod = methodMap.get(params.method());
        if(doQueryMethod == null) {
            throw new RuntimeException("Not Supported mdtp method.");
        }

        try {
            return (QueryResult) doQueryMethod.invoke(this, params);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    public List<DbValue> findAffectKey(AffectKey affectKey) {
        return dataLsmTree.find(affectKey.key(), affectKey.columnFamily());
    }

    public byte[] findTimeoutValue(TimeoutKey timeoutKey) {
        return timeoutLsmTree.find(
                timeoutKey.key(),
                timeoutKey.columnFamily(),
                TIMEOUT_COLUMN
        );
    }

    public void insertTimeoutKey(TimeoutValue timeoutValue) {
        TimeoutKey timeoutKey = timeoutValue.timeoutKey();

        timeoutLsmTree.insert(
                timeoutKey.key(),
                timeoutKey.columnFamily(),
                TIMEOUT_COLUMN,
                timeoutValue.value()
        );
    }

    public void removeTimeoutKey(TimeoutKey timeoutKey) {
        timeoutLsmTree.delete(
                timeoutKey.key(),
                timeoutKey.columnFamily(),
                TIMEOUT_COLUMN
        );
    }

    public void removeData(TimeoutKey timeoutKey) {
        dataLsmTree.delete(
                timeoutKey.key(),
                timeoutKey.columnFamily()
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
