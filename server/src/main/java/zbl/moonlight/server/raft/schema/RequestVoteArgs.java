package zbl.moonlight.server.raft.schema;

import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.core.utils.ByteArrayUtils;

import java.nio.ByteBuffer;

public class RequestVoteArgs {
    private final byte[] bytes;
    private final int term;
    private final int lastLogIndex;
    private final int lastLogTerm;

    public RequestVoteArgs(byte[] bytes) {
        this.bytes = bytes;
        Parser parser = new Parser(RequestVoteArgsSchema.class);
        parser.setByteBuffer(ByteBuffer.wrap(bytes));
        parser.parse();
        term = ByteArrayUtils.toInt(parser.mapGet(RaftSchemaEntryName.TERM));
        lastLogIndex = ByteArrayUtils.toInt(parser.mapGet(RaftSchemaEntryName.LAST_LOG_INDEX));
        lastLogTerm = ByteArrayUtils.toInt(parser.mapGet(RaftSchemaEntryName.LAST_LOG_TERM));
    }

    public int term() {
        return term;
    }

    public int lastLogIndex() {
        return lastLogIndex;
    }

    public int lastLogTerm() {
        return lastLogTerm;
    }
}
