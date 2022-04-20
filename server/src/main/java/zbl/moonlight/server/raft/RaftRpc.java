package zbl.moonlight.server.raft;

import zbl.moonlight.core.protocol.Serializer;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.protocol.nio.SocketState;
import zbl.moonlight.core.socket.SocketSchema;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.mdtp.MdtpRequest;
import zbl.moonlight.server.raft.log.RaftLog;
import zbl.moonlight.server.raft.log.RaftLogEntry;
import zbl.moonlight.server.raft.schema.AppendEntriesArgsSchema;
import zbl.moonlight.server.raft.schema.EntrySchema;
import zbl.moonlight.server.raft.schema.RequestVoteArgsSchema;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequestSchema;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.charset.StandardCharsets;

/**
 * Raft相关的RPC有两种：
 * （1）请求投票
 * （2）尾部添加日志
 * 这两种RPC请求都通过MDTP协议传输
 * key: [host]:[port] 例如：127.0.0.1:7820
 * value: 按照具体情况而定
 */
public class RaftRpc {
    private final static int DEFAULT_ENTRIES_SIZE = 1;

    private static int serial = 0;
    private static final Configuration config = MdtpServerContext.getInstance().getConfiguration();
    private static final byte[] key = (config.getHost() + ":" + config.getPort()).getBytes(StandardCharsets.UTF_8);

    private static final RaftState raftState = MdtpServerContext.getInstance().getRaftState();
    private static final RaftLog raftLog = raftState.getRaftLog();

    public static NioWriter newRequestVote(SelectionKey selectionKey) throws IOException {
        NioWriter writer = new NioWriter(MdtpRequestSchema.class, selectionKey);

        /* 序列化出value */
        Serializer serializer = new Serializer(RequestVoteArgsSchema.class, false);
        serializer.mapPut(RequestVoteArgsSchema.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(RequestVoteArgsSchema.LAST_LOG_INDEX, ByteArrayUtils.fromInt(raftState.lastApplied()));
        serializer.mapPut(RequestVoteArgsSchema.LAST_LOG_TERM, ByteArrayUtils.fromInt(raftState.lastLogTerm()));

        /* 设置writer的map值 */
        writer.mapPut(SocketSchema.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpRequestSchema.METHOD, new byte[]{MdtpMethod.REQUEST_VOTE});
        writer.mapPut(MdtpRequestSchema.SERIAL, ByteArrayUtils.fromInt(serial ++));
        writer.mapPut(MdtpRequestSchema.KEY, key);
        writer.mapPut(MdtpRequestSchema.VALUE, serializer.getByteBuffer().array());

        return writer;
    }

    public static NioWriter newAppendEntries(SelectionKey selectionKey, byte[] entries) throws IOException {
        NioWriter writer = new NioWriter(MdtpRequestSchema.class, selectionKey);

        RaftLogEntry prevLogEntry = raftLog.read(raftState.lastApplied());

        int commitIndex = 0, term = 0;
        if(prevLogEntry != null) {
            commitIndex = prevLogEntry.commitIndex();
            term = prevLogEntry.term();
        }

        byte[] prevLogIndex = ByteArrayUtils.fromInt(commitIndex);
        byte[] prevLogTerm = ByteArrayUtils.fromInt(term);
        byte[] leaderCommit = ByteArrayUtils.fromInt(raftState.lastApplied());

        /* 序列化出value */
        Serializer serializer = new Serializer(AppendEntriesArgsSchema.class, false);
        serializer.mapPut(AppendEntriesArgsSchema.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(AppendEntriesArgsSchema.PREV_LOG_INDEX, prevLogIndex);
        serializer.mapPut(AppendEntriesArgsSchema.PREV_LOG_TERM, prevLogTerm);
        serializer.mapPut(AppendEntriesArgsSchema.ENTRIES, entries);
        serializer.mapPut(AppendEntriesArgsSchema.LEADER_COMMIT, leaderCommit);

        writer.mapPut(SocketSchema.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpRequestSchema.METHOD, new byte[]{MdtpMethod.APPEND_ENTRIES});
        writer.mapPut(MdtpRequestSchema.SERIAL, ByteArrayUtils.fromInt(serial ++));
        writer.mapPut(MdtpRequestSchema.KEY, key);
        writer.mapPut(MdtpRequestSchema.VALUE, serializer.getByteBuffer().array());

        return writer;
    }

    public static NioWriter newAppendEntries(SelectionKey selectionKey, MdtpRequest request) throws IOException {
        Serializer serializer = new Serializer(EntrySchema.class);
        serializer.mapPut(EntrySchema.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(EntrySchema.COMMIT_INDEX, ByteArrayUtils.fromInt(raftState.nextApplied()));
        serializer.mapPut(EntrySchema.METHOD, new byte[]{request.method()});
        serializer.mapPut(EntrySchema.KEY, request.key());
        serializer.mapPut(EntrySchema.VALUE, request.value());

        byte[] entry = serializer.getByteBuffer().array();
        /* TODO:禁止魔数“4” */
        ByteBuffer entries = ByteBuffer.allocate(entry.length + 4);
        entries.putInt(DEFAULT_ENTRIES_SIZE);
        entries.put(entry);

        return newAppendEntries(selectionKey, entries.array());
    }

    public static NioWriter newRedirectMdtpRequest(SelectionKey selectionKey, MdtpRequest request) {
        NioWriter writer = new NioWriter(MdtpRequestSchema.class, selectionKey);

        /* 只有SET和DELETE方法需要重定向 */
        byte method = request.method() == MdtpMethod.SET ? MdtpMethod.REDIRECT_SET
                : MdtpMethod.REDIRECT_DELETE;

        writer.mapPut(MdtpRequestSchema.METHOD, new byte[]{method});
        writer.mapPut(MdtpRequestSchema.SERIAL, request.serial());
        writer.mapPut(MdtpRequestSchema.KEY, request.key());
        writer.mapPut(MdtpRequestSchema.VALUE, request.value());

        return writer;
    }

    public static NioWriter newHeartBeat(SelectionKey selectionKey) throws IOException {
        return newAppendEntries(selectionKey, new byte[0]);
    }
}
