package zbl.moonlight.server.context;

import zbl.moonlight.raft.state.RaftConfiguration;
import zbl.moonlight.socket.client.ServerNode;

public class MoonlightRaftConfiguration implements RaftConfiguration {
    @Override
    public String electionMode() {
        return Configuration.getInstance().electionMode();
    }

    @Override
    public ServerNode currentNode() {
        return Configuration.getInstance().currentNode();
    }
}
