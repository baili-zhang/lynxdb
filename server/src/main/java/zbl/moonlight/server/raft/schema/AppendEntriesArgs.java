package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.server.raft.log.RaftLogEntry;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AppendEntriesArgs {
    private final Parser parser;

    public AppendEntriesArgs(byte[] bytes) {
        parser = new Parser(AppendEntriesArgsSchema.class);
        parser.setByteBuffer(ByteBuffer.wrap(bytes));
        parser.parse();
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

    public void term() {
    }

    public void prevLogIndex() {
    }

    public void prevLogTerm() {
    }

    public void leaderCommit() {
    }
}
