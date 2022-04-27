package zbl.moonlight.core.raft.log;

import zbl.moonlight.core.socket.client.ServerNode;

public class TermLog {
    public int currentTerm() {
        return 0;
    }

    public ServerNode voteFor() {
        return null;
    }

    public void setCurrentTerm(int term) {
    }
}
