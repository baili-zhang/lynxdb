package zbl.moonlight.server.cluster;

import java.io.Serializable;

public class AppendEntriesRpc {
    public static class Arguments implements Serializable {
        int term;
        int leaderId;
        int prevLogIndex;
        int prevLogTerm;
        int entries;
        int leaderCommit;
    }

    public static class Results implements Serializable {
        int term;
        boolean success;
    }

    Results call(Arguments arg) {
        return null;
    }
}
