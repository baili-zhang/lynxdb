package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.lsmtree.LsmTree;
import com.bailizhang.lynxdb.lsmtree.LynxDbLsmTree;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;

/**
 * BaseStorageEngine：
 *  元数据存储
 *  KV 存储
 *  TABLE 存储
 */
public abstract class BaseStorageEngine {
    private static final int DEFAULT_MEM_TABLE_SIZE = 4000;

    protected final HashMap<Byte, Method> methodMap = new HashMap<>();
    protected final LsmTree lsmTree;

    /* TODO: Cache 以后再实现 */

    public BaseStorageEngine(Class<? extends BaseStorageEngine> clazz) {
        String dataDir = Configuration.getInstance().dataDir();
        Options options = new Options(DEFAULT_MEM_TABLE_SIZE);

        lsmTree = new LynxDbLsmTree(dataDir, options);

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
