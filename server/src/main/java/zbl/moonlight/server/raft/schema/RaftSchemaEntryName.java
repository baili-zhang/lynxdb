package zbl.moonlight.server.raft.schema;

public class RaftSchemaEntryName {
    public static final String TERM = "term";
    public static final String LAST_LOG_INDEX = "lastLogIndex";
    public static final String LAST_LOG_TERM = "lastLogTerm";
    public static final String PREV_LOG_INDEX = "prevLogIndex";
    public static final String PREV_LOG_TERM = "prevLogTerm";
    public static final String ENTRIES = "entries";
    public static final String LEADER_COMMIT = "leaderCommit";
}
