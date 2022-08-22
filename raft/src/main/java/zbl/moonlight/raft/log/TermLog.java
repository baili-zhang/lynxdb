package zbl.moonlight.raft.log;

import zbl.moonlight.core.enhance.EnhanceFile;
import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.socket.client.ServerNode;

import java.io.IOException;

/**
 * TermLog 用来记录当前任期和选举阶段的投票信息
 * 文件格式：
 * | currentTerm   | flag   | host length | host                | port    |
 * | ------------- | ------ | ----------- | ------------------- | ------- |
 * | 4 bytes       | 1 byte | 4 bytes     | (host length) bytes | 4 bytes |
 */
public class TermLog {
    private static final String DEFAULT_DIR = System.getProperty("user.dir") + "/logs";

    private static final int CURRENT_TERM_POSITION = 0;
    private static final int FLAG_POSITION = 4;
    private static final int VOTE_FOR_POSITION = 5;

    private static final byte VOTE_FOR_IS_NULL = (byte) 0x01;
    private static final byte VOTE_FOR_IS_NOT_NULL = (byte) 0x02;

    private final EnhanceFile termFile;

    public TermLog(String filename) throws IOException {
        termFile = new EnhanceFile(DEFAULT_DIR, filename);
        if(termFile.length() == 0) {
            termFile.writeInt(0, CURRENT_TERM_POSITION);
            termFile.writeByte(VOTE_FOR_IS_NULL, FLAG_POSITION);
        }
    }

    public int currentTerm() throws IOException {
        return termFile.readInt(CURRENT_TERM_POSITION);
    }

    public ServerNode voteFor() throws IOException {
        byte flag = termFile.readByte(FLAG_POSITION);

        if(flag == VOTE_FOR_IS_NULL) {
            return null;
        } else if(flag == VOTE_FOR_IS_NOT_NULL) {
            int len = termFile.readInt(VOTE_FOR_POSITION);
            String host = termFile.readString(VOTE_FOR_POSITION);
            int portPosition = VOTE_FOR_POSITION + NumberUtils.INT_LENGTH + len;
            int port = termFile.readInt(portPosition);
            return new ServerNode(host, port);
        } else {
            throw new RuntimeException("Flag is not [VOTE_FOR_IS_NULL] or [VOTE_FOR_IS_NOT_NULL].");
        }
    }

    public void setCurrentTerm(int term) throws IOException {
        termFile.writeInt(term, CURRENT_TERM_POSITION);
        termFile.writeByte(VOTE_FOR_IS_NULL, FLAG_POSITION);
    }

    public void setVoteFor(ServerNode node) throws IOException {
        if(node == null) {
            termFile.writeByte(VOTE_FOR_IS_NULL, FLAG_POSITION);
            return;
        }

        byte flag = termFile.readByte(FLAG_POSITION);
        if(flag == VOTE_FOR_IS_NULL) {
            termFile.writeByte(VOTE_FOR_IS_NOT_NULL, FLAG_POSITION);
        }

        long portPosition = termFile.writeString(node.host(), VOTE_FOR_POSITION);
        termFile.writeInt(node.port(), portPosition);
    }
}
