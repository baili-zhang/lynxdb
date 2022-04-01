package zbl.moonlight.server.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.Serializer;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.protocol.nio.SocketState;
import zbl.moonlight.core.socket.SocketSchema;
import zbl.moonlight.core.socket.SocketSchemaEntryName;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.raft.schema.AppendEntriesArgsSchema;
import zbl.moonlight.server.raft.schema.RaftSchemaEntryName;
import zbl.moonlight.server.raft.schema.RequestVoteArgsSchema;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequestSchema;
import zbl.moonlight.server.mdtp.MdtpSchemaEntryName;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

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
    private static Configuration config = MdtpServerContext.getInstance().getConfiguration();
    private static RaftState raftState = MdtpServerContext.getInstance().getRaftState();

    public static NioWriter newRequestVote(SelectionKey selectionKey) {
        NioWriter writer = new NioWriter(MdtpRequestSchema.class, selectionKey);
        String key = config.getHost() + ":" + config.getPort();

        /* 序列化出value */
        Serializer serializer = new Serializer(RequestVoteArgsSchema.class, false);
        serializer.mapPut(RaftSchemaEntryName.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(RaftSchemaEntryName.LAST_LOG_INDEX, ByteArrayUtils.fromInt(raftState.getLastApplied()));
        serializer.mapPut(RaftSchemaEntryName.LAST_LOG_TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));

        /* 设置writer的map值 */
        writer.mapPut(SocketSchemaEntryName.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpSchemaEntryName.METHOD, new byte[]{MdtpMethod.REQUEST_VOTE});
        writer.mapPut(MdtpSchemaEntryName.SERIAL, ByteArrayUtils.fromInt(serial ++));
        writer.mapPut(MdtpSchemaEntryName.KEY, key.getBytes(StandardCharsets.UTF_8));
        writer.mapPut(MdtpSchemaEntryName.VALUE, serializer.getByteBuffer().array());

        return writer;
    }

    public static NioWriter newAppendEntries(SelectionKey selectionKey) {
        NioWriter writer = new NioWriter(MdtpRequestSchema.class, selectionKey);
        String key = config.getHost() + ":" + config.getPort();

        /* 序列化出value */
        Serializer serializer = new Serializer(AppendEntriesArgsSchema.class, false);
        serializer.mapPut(RaftSchemaEntryName.TERM, ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        serializer.mapPut(RaftSchemaEntryName.PREV_LOG_INDEX, ByteArrayUtils.fromInt(0));
        serializer.mapPut(RaftSchemaEntryName.PREV_LOG_TERM, ByteArrayUtils.fromInt(0));
        serializer.mapPut(RaftSchemaEntryName.ENTRIES, "hallo".getBytes(StandardCharsets.UTF_8));
        serializer.mapPut(RaftSchemaEntryName.LEADER_COMMIT, ByteArrayUtils.fromInt(0));

        writer.mapPut(SocketSchemaEntryName.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpSchemaEntryName.METHOD, new byte[]{MdtpMethod.APPEND_ENTRIES});
        writer.mapPut(MdtpSchemaEntryName.SERIAL, ByteArrayUtils.fromInt(serial ++));
        writer.mapPut(MdtpSchemaEntryName.KEY, key.getBytes(StandardCharsets.UTF_8));
        writer.mapPut(MdtpSchemaEntryName.VALUE, "hallo".getBytes(StandardCharsets.UTF_8));

        return writer;
    }
}
