package zbl.moonlight.server.raft;

import zbl.moonlight.core.protocol.Serializer;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.protocol.nio.SocketState;
import zbl.moonlight.core.socket.SocketSchema;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.raft.log.RaftLog;
import zbl.moonlight.server.raft.log.RaftLogEntry;
import zbl.moonlight.server.raft.schema.AppendEntriesArgsSchema;
import zbl.moonlight.server.raft.schema.RequestVoteArgsSchema;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequestSchema;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.io.IOException;
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

    public static NioWriter newAppendEntries(SelectionKey selectionKey, byte[] entry) throws IOException {
        NioWriter writer = new NioWriter(MdtpRequestSchema.class, selectionKey);

        RaftLogEntry prevLogEntry = raftLog.read(raftState.lastApplied());
        byte[] prevLogIndex = ByteArrayUtils.fromInt(prevLogEntry.commitIndex());
        byte[] prevLogTerm = ByteArrayUtils.fromInt(prevLogEntry.term());
        byte[] leaderCommit = ByteArrayUtils.fromInt(raftState.lastApplied());

        /* 序列化出value */
        Serializer serializer = new Serializer(AppendEntriesArgsSchema.class, false);
        serializer.mapPut(AppendEntriesArgsSchema.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(AppendEntriesArgsSchema.PREV_LOG_INDEX, prevLogIndex);
        serializer.mapPut(AppendEntriesArgsSchema.PREV_LOG_TERM, prevLogTerm);
        serializer.mapPut(AppendEntriesArgsSchema.ENTRIES, entry);
        serializer.mapPut(AppendEntriesArgsSchema.LEADER_COMMIT, leaderCommit);

        writer.mapPut(SocketSchema.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpRequestSchema.METHOD, new byte[]{MdtpMethod.APPEND_ENTRIES});
        writer.mapPut(MdtpRequestSchema.SERIAL, ByteArrayUtils.fromInt(serial ++));
        writer.mapPut(MdtpRequestSchema.KEY, key);
        writer.mapPut(MdtpRequestSchema.VALUE, serializer.getByteBuffer().array());

        return writer;
    }

    public static NioWriter newAppendEntries(SelectionKey selectionKey,
                                             RaftLogEntry entry) throws IOException {
        return newAppendEntries(selectionKey, entry.serializeEntry());
    }

    public static NioWriter newHeartBeat(SelectionKey selectionKey) throws IOException {
        return newAppendEntries(selectionKey, new byte[0]);
    }
}
