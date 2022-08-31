package com.bailizhang.lynxdb.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.server.annotations.MdtpMethod;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.storage.core.KvAdapter;
import com.bailizhang.lynxdb.storage.core.TableAdapter;
import com.bailizhang.lynxdb.storage.rocks.RocksKvAdapter;
import com.bailizhang.lynxdb.storage.rocks.RocksTableAdapter;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;

/**
 * BaseStorageEngine：
 *  元数据存储
 *  KV 存储
 *  TABLE 存储
 */
public abstract class BaseStorageEngine {
    private static final Logger logger = LogManager.getLogger("StorageEngine");

    private static final String KV_DIR = "kv";
    private static final String TABLE_DIR = "table";
    private static final String META_DIR = "meta_info";

    public static final String META_DB_NAME = "raft_meta";

    protected final KvAdapter metaDb;

    protected final HashMap<Byte, Method> methodMap = new HashMap<>();
    protected final HashMap<String, KvAdapter> kvDbMap = new HashMap<>();
    protected final HashMap<String, TableAdapter> tableMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public BaseStorageEngine(Class<? extends BaseStorageEngine> clazz) {
        String dataDir = Configuration.getInstance().dataDir();
        String metaDbDir = Path.of(dataDir, META_DIR, META_DB_NAME).toString();

        metaDb = new RocksKvAdapter(metaDbDir);

        initKvDb();
        initTable();
        initMethod(clazz);
    }

    public byte[] doQuery(QueryParams params) {
        Method doQueryMethod = methodMap.get(params.method());
        if(doQueryMethod == null) {
            throw new RuntimeException("Not Supported mdtp method.");
        }

        try {
            return (byte[]) doQueryMethod.invoke(this, params);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }

    private void initKvDb() {
        String dataDir = Configuration.getInstance().dataDir();
        Path kvPath = Path.of(dataDir, KV_DIR);

        File kvDir = kvPath.toFile();

        if(!kvDir.isDirectory() && !kvDir.mkdir()) {
            throw new RuntimeException("Kv dir create failed");
        }

        String[] kvDbNames = kvDir.list();

        if(kvDbNames == null) {
            return;
        }

        for (String kvDbName : kvDbNames) {
            File subFile = Path.of(kvDir.getPath(), kvDbName).toFile();
            if(subFile.isDirectory()) {
                kvDbMap.put(kvDbName, new RocksKvAdapter(subFile.getPath()));
            }
        }
    }

    private void initTable() {
        String dataDir = Configuration.getInstance().dataDir();
        Path tablePath = Path.of(dataDir, TABLE_DIR);

        File tableDir = tablePath.toFile();

        if(!tableDir.isDirectory() && !tableDir.mkdir()) {
            throw new RuntimeException("Kv dir create failed");
        }

        String[] tableDbNames = tableDir.list();

        if(tableDbNames == null) {
            return;
        }

        for (String tableDbName : tableDbNames) {
            File subFile = Path.of(tableDir.getPath(), tableDbName).toFile();
            if(subFile.isDirectory()) {
                tableMap.put(tableDbName, new RocksTableAdapter(subFile.getPath()));
            }
        }
    }

    private void initMethod(Class<? extends BaseStorageEngine> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        Arrays.stream(methods).forEach(method -> {
            MdtpMethod mdtpMethod = method.getAnnotation(MdtpMethod.class);

            if(mdtpMethod == null) {
                return;
            }

            methodMap.put(mdtpMethod.value(), method);
        });
    }

    protected void createKvDb(String name) {
        String dataDir = Configuration.getInstance().dataDir();
        Path path = Path.of(dataDir, KV_DIR, name);

        File dir = path.toFile();

        if(!dir.exists() || !dir.isDirectory()) {
            if(!dir.mkdir()) {
                throw new RuntimeException("Kv dir [" + name + "] create failed");
            }
        }

        kvDbMap.put(name, new RocksKvAdapter(dir.getPath()));
    }

    protected void dropKvDb(String name) {
        String dataDir = Configuration.getInstance().dataDir();
        Path path = Path.of(dataDir, KV_DIR, name);

        try {
            kvDbMap.get(name).close();
            FileUtils.delete(path);
        } catch (Exception e) {
            throw new RuntimeException("Drop kvstore [" + name + "] fail");
        }

        kvDbMap.remove(name);
    }

    protected void createTableDb(String name) {
        String dataDir = Configuration.getInstance().dataDir();
        Path path = Path.of(dataDir, TABLE_DIR, name);

        File dir = path.toFile();

        if(!dir.exists() || !dir.isDirectory()) {
            if(!dir.mkdir()) {
                throw new RuntimeException("Table dir [" + name + "] create failed");
            }
        }

        tableMap.put(name, new RocksTableAdapter(dir.getPath()));
    }

    protected void dropTableDb(String name) {
        String dataDir = Configuration.getInstance().dataDir();
        Path path = Path.of(dataDir, TABLE_DIR, name);

        try {
            tableMap.get(name).close();
            FileUtils.delete(path);
        } catch (Exception e) {
            throw new RuntimeException("Drop kvstore [" + name + "] fail");
        }

        tableMap.remove(name);
    }
}
