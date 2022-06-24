package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.raft.request.Entry;
import zbl.moonlight.core.raft.state.StateMachine;
import zbl.moonlight.server.storage.EngineExecutor;

import java.nio.channels.SelectionKey;

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
    public void apply(Entry[] entries) {
        if(engineExecutor == null) {
            throw new RuntimeException("[storageEngine] is [null]");
        }
        for(Entry entry : entries) {
            MdtpCommand command = new MdtpCommand(null, entry.command());
            logger.info("Apply command {} to state machine.", command);
            engineExecutor.offerInterruptibly(command);
        }
    }

    @Override
    public void exec(SelectionKey key, byte[] command) {
        if(engineExecutor == null) {
            throw new RuntimeException("[storageEngine] is [null]");
        }
        MdtpCommand mdtpCommand = new MdtpCommand(key, command);
        engineExecutor.offerInterruptibly(mdtpCommand);
    }
}
