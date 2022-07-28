package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.RocksDBException;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.server.context.Configuration;
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.storage.core.Pair;
import zbl.moonlight.storage.core.ResultSet;
import zbl.moonlight.storage.rocks.RocksDatabase;
import zbl.moonlight.storage.rocks.RocksKvAdapter;
import zbl.moonlight.storage.rocks.query.kv.KvBatchGetQuery;

import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 异步的状态机
 * 直接把 command 解析后转发给 storageEngine
 */
public class MdtpStateMachine implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    private static final String META_DIR = "meta_info";
    public static final String META_DB_NAME = "raft_meta";
    private static final byte[] CLUSTER_NODES = "cluster_nodes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] NEW_CLUSTER_NODES = "new_cluster_nodes".getBytes(StandardCharsets.UTF_8);

    private final RocksKvAdapter kvDb;
    private final ServerNode currentNode;

    private EngineExecutor engineExecutor;

    public MdtpStateMachine() {
        Configuration config = Configuration.getInstance();
        String dataDir = config.dataDir();
        String metaDir = Path.of(dataDir, META_DIR).toString();

        kvDb = new RocksKvAdapter(META_DB_NAME, metaDir);
        currentNode = config.currentNode();
    }

    public void setStorageEngine(EngineExecutor engine) {
        engineExecutor = engine;
    }

    @Override
    public List<ServerNode> clusterNodes() {
        byte[] value = kvDb.get(CLUSTER_NODES);
        if(value == null) {
            List<ServerNode> clusterNodes = List.of(currentNode);
            kvDb.set(new Pair<>(CLUSTER_NODES, clusterNodesToBytes(clusterNodes)));
            return clusterNodes;
        }
        return parseClusterNodes(value);
    }

    @Override
    public List<ServerNode> newClusterNodes() {
        return parseClusterNodes(kvDb.get(NEW_CLUSTER_NODES));
    }

    @Override
    public void newClusterNodes(List<ServerNode> clusterNodes) {
        kvDb.set(new Pair<>(NEW_CLUSTER_NODES, clusterNodesToBytes(clusterNodes)));
    }

    /**
     * TODO: 需要数据库的事务支持
     */
    @Override
    public void changeClusterNodes() {
    }

    private byte[] clusterNodesToBytes(List<ServerNode> currentNodes) {
        String total = currentNodes.stream().map(ServerNode::toString)
                .collect(Collectors.joining(" "));
        return total.getBytes(StandardCharsets.UTF_8);
    }

    private List<ServerNode> parseClusterNodes(byte[] value) {
        if(value == null) {
            return null;
        }

        String total = new String(value);
        String[] nodes = total.trim().split("\\s+");
        return Arrays.stream(nodes).map(ServerNode::parse).toList();
    }

    @Override
    public void apply(Entry[] entries) {
        if(engineExecutor == null) {
            throw new RuntimeException("[storageEngine] is [null]");
        }
        for(Entry entry : entries) {
            logger.info("Apply command {} to state machine.", entry.command());
            engineExecutor.offerInterruptibly(null);
        }
    }

    /**
     * TODO: 异步执行会不会存在数据丢失的问题？
     *
     * 客户端 -> Raft 层 -> 状态机 -> Raft 层 -> 客户端
     *
     * @param key SelectionKey
     * @param command 命令
     */
    @Override
    public void exec(SelectionKey key, byte[] command) {
        if(engineExecutor == null) {
            throw new RuntimeException("[storageEngine] is [null]");
        }

        engineExecutor.offerInterruptibly(null);
    }
}
