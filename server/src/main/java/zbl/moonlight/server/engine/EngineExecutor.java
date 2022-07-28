package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.server.RaftServer;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpCommand;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.socket.response.WritableSocketResponse;
import zbl.moonlight.storage.core.KvAdapter;
import zbl.moonlight.storage.core.TableAdapter;
import zbl.moonlight.storage.rocks.RocksKvAdapter;
import zbl.moonlight.storage.rocks.query.Query;

import java.io.File;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;

public class EngineExecutor extends Executor<MdtpCommand> {
    private static final Logger logger = LogManager.getLogger("StorageEngine");

    private static final String KV = "kv";
    private static final String TABLE = "table";

    private final RaftServer raftServer;
    private final HashMap<Byte, Method> methodMap = new HashMap<>();

    protected final HashMap<String, KvAdapter> kvDbMap = new HashMap<>();
    protected final HashMap<String, TableAdapter> tableMap = new HashMap<>();

    /* TODO: Cache 以后再实现 */

    public EngineExecutor(RaftServer server, Class<? extends EngineExecutor> clazz) {
        raftServer = server;

        initKvDb();
        initTable();
        initMethod(clazz);
    }

    private void initKvDb() {
        String dataDir = Configuration.getInstance().dataDir();
        Path kvPath = Path.of(dataDir, KV);

        File kvDir = kvPath.toFile();


        if(kvDir.isDirectory()) {
            String[] kvDbNames = kvDir.list();

            if(kvDbNames == null) {
                return;
            }

            for (String kvDbName : kvDbNames) {
                File subFile = Path.of(kvDir.getPath(), kvDbName).toFile();
                if(subFile.isDirectory()) {
                    kvDbMap.put(kvDbName, new RocksKvAdapter(kvDbName, kvDir.getPath()));
                }
            }
        }
    }

    private void initTable() {
    }

    private void initMethod(Class<? extends EngineExecutor> clazz) {
        Method[] methods = clazz.getDeclaredMethods();
        Arrays.stream(methods).forEach(method -> {
            MdtpMethod mdtpMethod = method.getAnnotation(MdtpMethod.class);
            methodMap.put(mdtpMethod.value(), method);
        });
    }

    @Override
    protected void execute() {
        MdtpCommand mdtpCommand = blockPoll();
        if(mdtpCommand == null) {
            return;
        }

        Method method = methodMap.get(mdtpCommand.method());
        if(method == null) {
            // TODO: 处理不支持的方法类型
            return;
        }

        try {
            WritableSocketResponse response = (WritableSocketResponse) method
                    .invoke(this, mdtpCommand);
            // 将响应发送给 raftServer
            raftServer.offerInterruptibly(response);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
