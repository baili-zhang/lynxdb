package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.Key;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.core.RocksDatabase;
import zbl.moonlight.storage.query.GetQuery;
import zbl.moonlight.storage.query.QueryTuple;

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
    private static final Key CLUSTER_NODES_KEY = new Key(
            "cluster_nodes_key".getBytes(StandardCharsets.UTF_8)
    );
    private static final ColumnFamily CLUSTER_NODES_COLUMN_FAMILY = new ColumnFamily(
            "cluster_nodes_column_family".getBytes(StandardCharsets.UTF_8)
    );
    private static final ColumnFamily CLUSTER_NODES_COLUMN_FAMILY_NEW = new ColumnFamily(
            "cluster_nodes_column_family_new".getBytes(StandardCharsets.UTF_8)
    );

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
        byte[] value = metaDbGet(CLUSTER_NODES_COLUMN_FAMILY);
        return parseClusterNodes(value);
    }

    @Override
    public List<ServerNode> newClusterNodes() {
        byte[] value = metaDbGet(CLUSTER_NODES_COLUMN_FAMILY_NEW);
        return parseClusterNodes(value);
    }

    private List<ServerNode> parseClusterNodes(byte[] value) {
        return null;
    }

    private byte[] metaDbGet(ColumnFamily columnFamily) {
        try {
            QueryTuple queryTuple = new QueryTuple(MdtpStateMachine.CLUSTER_NODES_KEY,
                    columnFamily, null);
            ResultSet resultSet = metaDb.doQuery(new GetQuery(List.of(queryTuple)));
            if(resultSet.code() != ResultSet.SUCCESS) {
                throw new RuntimeException("Get clusterNodes failed.");
            }
            List<QueryTuple> result = resultSet.result();
            return result.get(0).valueBytes();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
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
