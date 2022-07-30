package zbl.moonlight.raft.request;

import zbl.moonlight.core.exceptions.NullFieldException;
import zbl.moonlight.raft.log.Entry;
import zbl.moonlight.socket.client.ServerNode;
import zbl.moonlight.socket.request.SocketRequest;

import java.util.List;

public abstract class RaftRequest extends SocketRequest {
    public final static byte REQUEST_VOTE = (byte) 0x01;
    public final static byte APPEND_ENTRIES = (byte) 0x02;
    public final static byte CLIENT_REQUEST = (byte) 0x03;

    private Integer term;
    private ServerNode leader;
    private Integer prevLogIndex;
    private Integer prevLogTerm;
    private List<Entry> entries;
    private Integer leaderCommit;

    private ServerNode candidate;
    private Integer lastLogIndex;
    private Integer lastLogTerm;

    protected RaftRequest() {
    }

    public int term() {
        if(term == null) {
            throw new NullFieldException("term");
        }
        return term;
    }

    public void term(int val) {
        if(term == null) {
            term = val;
        }
    }

    public ServerNode leader() {
        if(leader == null) {
            throw new NullFieldException("leader");
        }
        return leader;
    }

    public void leader(ServerNode val) {
        if(leader == null) {
            leader = val;
        }
    }
    public int prevLogIndex() {
        if(prevLogIndex == null) {
            throw new NullFieldException("prevLogIndex");
        }
        return prevLogIndex;
    }

    public void prevLogIndex(int val) {
        if(prevLogIndex == null) {
            prevLogIndex = val;
        }
    }
    public int prevLogTerm() {
        if(prevLogTerm == null) {
            throw new NullFieldException("prevLogTerm");
        }
        return prevLogTerm;
    }

    public void prevLogTerm(int val) {
        if(prevLogTerm == null) {
            prevLogTerm = val;
        }
    }
    public List<Entry> entries() {
        if(entries == null) {
            throw new NullFieldException("entries");
        }
        return entries;
    }

    public void entries(List<Entry> val) {
        if(entries == null) {
            entries = val;
        }
    }
    public int leaderCommit() {
        if(leaderCommit == null) {
            throw new NullFieldException("leaderCommit");
        }
        return leaderCommit;
    }

    public void leaderCommit(int val) {
        if(leaderCommit == null) {
            leaderCommit = val;
        }
    }
    public ServerNode candidate() {
        if(candidate == null) {
            throw new NullFieldException("candidate");
        }
        return candidate;
    }

    public void candidate(ServerNode val) {
        if(candidate == null) {
            candidate = val;
        }
    }
    public int lastLogIndex() {
        if(lastLogIndex == null) {
            throw new NullFieldException("lastLogIndex");
        }
        return lastLogIndex;
    }

    public void lastLogIndex(int val) {
        if(lastLogIndex == null) {
            lastLogIndex = val;
        }
    }

    public int lastLogTerm() {
        if(lastLogTerm == null) {
            throw new NullFieldException("lastLogTerm");
        }
        return lastLogTerm;
    }

    public void lastLogTerm(int val) {
        if(lastLogTerm == null) {
            lastLogTerm = val;
        }
    }
}
