package zbl.moonlight.server.mdtp;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.raft.state.RaftCommand;
import zbl.moonlight.raft.state.StateMachine;
import zbl.moonlight.server.engine.MdtpStorageEngine;
import zbl.moonlight.socket.client.ServerNode;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TODO: 异步执行会不会存在数据丢失的问题？
 *
 * 客户端 -> Raft 层 -> 状态机 -> Raft 层 -> 客户端
 */
public class MdtpStateMachine extends Executor<RaftCommand> implements StateMachine {
    private static final Logger logger = LogManager.getLogger("MdtpStateMachine");

    /**
     * 定义成 static final 类型，无论实例化多少个 MdtpStateMachine，都只有一个 MdtpStorageEngine 实例
     */
    private static final MdtpStorageEngine storageEngine = new MdtpStorageEngine();

    public MdtpStateMachine() {

    }

    @Override
    public List<ServerNode> clusterNodes() {
        byte[] cOldNew = storageEngine.metaGet(MdtpStorageEngine.C_OLD_NEW);

        if(cOldNew == null) {
            byte[] c = storageEngine.metaGet(MdtpStorageEngine.C);
            return parseClusterNodes(c);
        }

        return parseClusterNodes(cOldNew);
    }

    @Override
    public void apply(Entry[] entries) {

    }

    @Override
    protected void execute() {

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
}
