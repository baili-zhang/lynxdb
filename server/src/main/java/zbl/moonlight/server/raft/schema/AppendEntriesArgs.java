package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.raft.log.RaftLogEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AppendEntriesArgs {
    private final int term;
    private final int preLogIndex;
    private final int preLogTerm;
    private final int leaderCommit;

    private final Parser parser;

    public AppendEntriesArgs(byte[] bytes) {
        parser = new Parser(AppendEntriesArgsSchema.class);
        parser.setByteBuffer(ByteBuffer.wrap(bytes));
        parser.parse();
        term = ByteArrayUtils.toInt(parser.mapGet(AppendEntriesArgsSchema.TERM));
        preLogIndex = ByteArrayUtils.toInt(parser.mapGet(AppendEntriesArgsSchema.PREV_LOG_INDEX));
        preLogTerm = ByteArrayUtils.toInt(parser.mapGet(AppendEntriesArgsSchema.PREV_LOG_TERM));
        leaderCommit = ByteArrayUtils.toInt(parser.mapGet(AppendEntriesArgsSchema.LEADER_COMMIT));
    }

    public List<RaftLogEntry> entries() {
        List<RaftLogEntry> entries = new ArrayList<>();
        ByteBuffer buffer = ByteBuffer.wrap(
                parser.mapGet(AppendEntriesArgsSchema.ENTRIES)
        );

        int size = buffer.getInt();
        for (int i = 0; i < size; i ++) {
            int len = buffer.getInt();
            byte[] bytes = new byte[len];
            buffer.get(bytes);

            entries.add(RaftLogEntry.parseFrom(bytes));
        }

        return entries;
    }

    public int term() {
        return term;
    }

    public int prevLogIndex() {
        return preLogIndex;
    }

    public int prevLogTerm() {
        return preLogTerm;
    }

    public int leaderCommit() {
        return leaderCommit;
    }
}
