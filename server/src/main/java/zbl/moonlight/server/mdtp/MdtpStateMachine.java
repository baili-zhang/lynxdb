package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.server.engine.EngineExecutor;
import zbl.moonlight.socket.client.ServerNode;

import java.nio.channels.SelectionKey;
import java.util.HashMap;
import java.util.List;

/**
 * 异步的状态机
 * 直接把 command 解析后转发给 storageEngine
 */
public class MdtpStateMachine implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    private EngineExecutor engineExecutor;

    public void setStorageEngine(EngineExecutor engine) {
        engineExecutor = engine;
    }

    @Override
    public List<ServerNode> clusterNodes() {
        return null;
    }

    @Override
    public ServerNode currentNode() {
        return null;
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
