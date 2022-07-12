package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.socket.server.SocketServer;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.server.mdtp.Method;
import zbl.moonlight.server.mdtp.Params;
import zbl.moonlight.storage.core.Database;
import zbl.moonlight.storage.query.Queryable;
import zbl.moonlight.storage.core.ResultSet;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EngineExecutor extends Executor<Map<String, Object>> {
    private static final Logger logger = LogManager.getLogger("StorageEngine");

    private final SocketServer socketServer;
    private final HashMap<String, KvDatabase> kvDbs = new HashMap<>();
    private final HashMap<String, Database> cfDbs = new HashMap<>();

    private final HashMap<Byte, Class<?>> methodMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public EngineExecutor(SocketServer socketServer) {
        this.socketServer = socketServer;

        String dataDir = Configuration.getInstance().dataDir();
        Path KvPath = Path.of(dataDir, KvDatabase.KV_DIR);
        Path CfPath = Path.of(dataDir, Database.CF_DIR);

        File kvDbDir = KvPath.toFile();
        File cfDbDir = CfPath.toFile();

        if(kvDbDir.isDirectory()) {
            String[] subs = kvDbDir.list();

            if(subs == null) {
                return;
            }

            for (String sub : subs) {
                File subFile = Path.of(kvDbDir.getPath(), sub).toFile();
                if(subFile.isDirectory()) {
                    kvDbs.put(sub, new KvDatabase(sub, KvPath.toString()));
                }
            }
        }

        if(cfDbDir.isDirectory()) {
            String[] subs = cfDbDir.list();

            if(subs == null) {
                return;
            }

            for (String sub : subs) {
                File subFile = Path.of(cfDbDir.getPath(), sub).toFile();
                if(subFile.isDirectory()) {
                    cfDbs.put(sub, new Database(sub, CfPath.toString()));
                }
            }
        }

        Method[] methods = Method.values();
        for(Method method : methods) {
            methodMap.put(method.value(), method.type());
        }
    }

    @Override
    protected void doAfterShutdown() {

    }

    @Override
    protected void execute() {
        Map<String, Object> map = blockPoll();
        if(map == null) {
            return;
        }

        Queryable query = null;
        Byte methodByte = (Byte) map.get(Params.METHOD);
        Class<?> type = methodMap.get(methodByte);

        if(type != null) {
            Constructor<?>[] constructors = type.getDeclaredConstructors();

            if(constructors.length < 1) {
                throw new RuntimeException(type.getName() + " has no constructor");
            }
            Constructor<?> constructor = constructors[0];

            Type[] types = constructor.getGenericParameterTypes();
            List<Object> args = new ArrayList<>();

            for(Type t : types) {
                args.add(map.get(t.getTypeName()));
            }

            try {
                query = (Queryable) constructor.newInstance(args.toArray(Object[]::new));
            } catch (InstantiationException | IllegalAccessException | InvocationTargetException e) {
                e.printStackTrace();
            }
        }

        if(query == null) {
            return;
        }

        String dbName = (String) map.get("db_name");
        String dbType = (String) map.get("db_type");

        ResultSet resultSet = null;

        if("kv".equals(dbType)) {
            KvDatabase db = kvDbs.get(dbName);
            resultSet = db.doQuery(query);
        } else {
            KvDatabase db = kvDbs.get(dbName);
            resultSet = db.doQuery(query);
        }

        socketServer.offerInterruptibly(null);
    }
}
