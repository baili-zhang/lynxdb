package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.RocksDatabase;
import zbl.moonlight.storage.rocks.query.kv.KvBatchGetQuery;

import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;

/**
 * 异步的状态机
 * 直接把 command 解析后转发给 storageEngine
 */
public class MdtpStateMachine implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    public static final String META_DB_NAME = "raft_meta";
    private static final byte[] CLUSTER_NODES = "cluster_nodes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW_CLUSTER_NODES = "new_cluster_nodes".getBytes(StandardCharsets.UTF_8);

    private EngineExecutor engineExecutor;
    private final RocksDatabase metaDb;

    public MdtpStateMachine() {
        String dataDir = Configuration.getInstance().dataDir();
        try {
            metaDb = RocksDatabase.open(META_DB_NAME, dataDir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void setStorageEngine(EngineExecutor engine) {
        engineExecutor = engine;
    }

    @Override
    public List<ServerNode> clusterNodes() {
        byte[] value = metaDbGet(CLUSTER_NODES);
        return parseClusterNodes(value);
    }

    @Override
    public List<ServerNode> newClusterNodes() {
        byte[] value = metaDbGet(NEW_CLUSTER_NODES);
        return parseClusterNodes(value);
    }

    private List<ServerNode> parseClusterNodes(byte[] value) {
        return null;
    }

    private byte[] metaDbGet(byte[] key) {
        return null;
    }

    @Override
    public void changeClusterNodes() {

    }

    @Override
    public void apply(Entry[] entries) {
        if(engineExecutor == null) {
            throw new RuntimeException("[storageEngine] is [null]");
        }
        for(Entry entry : entries) {
            logger.info("Apply command {} to state machine.", entry.command());
            engineExecutor.offerInterruptibly(new HashMap<>());
        }
    }

    @Override
    public void exec(SelectionKey key, byte[] command) {
        if(engineExecutor == null) {
            throw new RuntimeException("[storageEngine] is [null]");
        }

        engineExecutor.offerInterruptibly(new HashMap<>());
    }
}
