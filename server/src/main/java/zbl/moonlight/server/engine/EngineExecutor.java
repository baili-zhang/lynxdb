package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.socket.server.SocketServer;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.server.mdtp.Method;
import zbl.moonlight.server.mdtp.Params;
import zbl.moonlight.storage.core.RocksDatabase;
import zbl.moonlight.storage.query.Query;
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
    private final HashMap<String, RocksDatabase> dbMap = new HashMap<>();

    private final HashMap<Byte, Class<?>> methodMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public EngineExecutor(SocketServer socketServer) {
        this.socketServer = socketServer;

        String dataDir = Configuration.getInstance().dataDir();
        Path CfPath = Path.of(dataDir);

        File cfDbDir = CfPath.toFile();


        if(cfDbDir.isDirectory()) {
            String[] subs = cfDbDir.list();

            if(subs == null) {
                return;
            }

            for (String sub : subs) {
                File subFile = Path.of(cfDbDir.getPath(), sub).toFile();
                if(subFile.isDirectory()) {
                    try {
                        dbMap.put(sub, RocksDatabase.open(sub, CfPath.toString()));
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        Method[] methods = Method.values();
        for(Method method : methods) {
            methodMap.put(method.value(), method.type());
        }
    }

    @Override
    protected void execute() {
        Map<String, Object> map = blockPoll();
        if(map == null) {
            return;
        }

        Query query = null;
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
                query = (Query) constructor.newInstance(args.toArray(Object[]::new));
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

        RocksDatabase db = dbMap.get(dbName);
        try {
            resultSet = db.doQuery(query);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }

        // 将响应发送给 raftServer
        socketServer.offerInterruptibly(null);
    }
}
