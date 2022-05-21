package zbl.moonlight.server.storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.raft.response.RaftResponse;
import zbl.moonlight.core.socket.response.SocketResponse;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.server.mdtp.MdtpCommand;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Map;

import static zbl.moonlight.server.mdtp.MdtpCommand.*;

public class StorageEngine extends Executor<MdtpCommand> {
    private static final Logger logger = LogManager.getLogger("Engine");

    private final SocketServer socketServer;
    private final Map<String, byte[]> storage;
    /* TODO: Cache 以后再实现 */

    /* 是否关闭 */
    private boolean shutdown = false;

    public StorageEngine(SocketServer socketServer, Map<String, byte[]> storage) {
        this.socketServer = socketServer;
        this.storage = storage;
    }

    public void shutdown () {
        shutdown = true;
    }

    @Override
    public final void run() {
        while (!shutdown) {
            /* 阻塞 poll，需要被中断 */
            MdtpCommand command = blockPoll();
            SocketResponse response = exec(command);
            /* Raft 日志 apply 的 command 执行后返回的 response 为 null */
            if(response != null) {
                socketServer.offerInterruptibly(response);
            }
        }
    }

    private SocketResponse exec(MdtpCommand command) {
        switch (command.method()) {
            case SET -> { return doSet(command); }
            case GET -> { return doGet(command); }
            case DELETE -> { return doDelete(command); }
            default -> throw new RuntimeException("Unsupported method.");
        }
    }

    private SocketResponse doSet(MdtpCommand command) {
        storage.put(command.key(), command.value());
        if(command.selectionKey() == null) {
            return null;
        }
        return success(command.selectionKey());
    }

    private SocketResponse doGet(MdtpCommand command) {
        SelectionKey selectionKey = command.selectionKey();
        if(selectionKey == null) {
            throw new RuntimeException("selectionKey can not be [null]");
        }

        byte[] value = storage.get(command.key());
        return successWithValue(selectionKey, value);
    }

    private SocketResponse doDelete(MdtpCommand command) {
        storage.remove(command.key());
        if(command.selectionKey() == null) {
            return null;
        }
        return success(command.selectionKey());
    }

    private SocketResponse success(SelectionKey selectionKey) {
        byte[] data = RaftResponse.clientRequestSuccessWithoutResult();
        return new SocketResponse(selectionKey, data, null);
    }

    private SocketResponse successWithValue(SelectionKey selectionKey, byte[] value) {
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH + value.length);
        byte[] data = buffer.putInt(value.length).put(value).array();
        return new SocketResponse(selectionKey, data, null);
    }
}
