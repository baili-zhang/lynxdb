/*
 * Copyright 2022-2024 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.recorder.FlightDataRecorder;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import com.bailizhang.lynxdb.server.measure.MeasureOptions;
import com.bailizhang.lynxdb.table.LynxDbTable;
import com.bailizhang.lynxdb.table.Table;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.config.TableOptions;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Callable;


public class BaseStorageEngine {
    private static final int DEFAULT_MEM_TABLE_SIZE = 4000;

    protected final Table dataTable;
    protected final HashMap<Byte, Method> methodMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public BaseStorageEngine(Class<? extends BaseStorageEngine> clazz) {
        Configuration config = Configuration.getInstance();
        String dataDir = config.dataDir();

        LsmTreeOptions lsmTreeOptions = new LsmTreeOptions(DEFAULT_MEM_TABLE_SIZE);
        dataTable = new LynxDbTable(new TableOptions(dataDir, lsmTreeOptions));

        initMethod(clazz);
    }

    public synchronized QueryResult doQuery(QueryParams params) {
        Method doQueryMethod = methodMap.get(params.method());
        if(doQueryMethod == null) {
            throw new RuntimeException("Not supported ldtp method.");
        }

        FlightDataRecorder recorder = FlightDataRecorder.recorder();
        recorder.count(MeasureOptions.ENGINE_QUERY_COUNT);

        try {
            if(recorder.isEnable()) {
                return (QueryResult) recorder.record(
                        (Callable<Object>) () -> doQueryMethod.invoke(this, params),
                        MeasureOptions.ENGINE_DO_QUERY_TIME
                );
            } else {
                return (QueryResult) doQueryMethod.invoke(this, params);
            }
        } catch (Exception e) {
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
