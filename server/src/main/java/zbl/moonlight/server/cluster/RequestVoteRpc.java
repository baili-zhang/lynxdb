package zbl.moonlight.server.cluster;

import java.io.Serializable;
import java.nio.ByteBuffer;

public record RequestVoteRpc(RaftNode node) {
    public record Arguments(int term,
                            int candidateId,
                            int lastLogIndex,
                            int lastLogTerm,
                            byte[] bytes) implements Serializable {
    }

    public static class Results implements Serializable {
        int term;
        int voteGranted;
    }

    Results call(Arguments arg) {
        return null;
    }
}
