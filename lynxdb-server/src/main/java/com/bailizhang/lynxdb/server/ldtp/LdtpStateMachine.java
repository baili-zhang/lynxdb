package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.core.common.CheckThreadSafety;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.ArrayUtils;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.ldtp.annotations.LdtpCode;
import com.bailizhang.lynxdb.raft.core.ClientRequest;
import com.bailizhang.lynxdb.raft.result.JoinClusterResult;
import com.bailizhang.lynxdb.raft.server.RaftServer;
import com.bailizhang.lynxdb.raft.spi.StateMachine;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.socket.client.ServerNode;
import com.bailizhang.lynxdb.socket.response.WritableSocketResponse;
import com.bailizhang.lynxdb.table.LynxDbTable;
import com.bailizhang.lynxdb.table.Table;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.JOIN_CLUSTER;
import static com.bailizhang.lynxdb.ldtp.request.RaftRpc.LEAVE_CLUSTER;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.LDTP_METHOD;
import static com.bailizhang.lynxdb.ldtp.request.RequestType.RAFT_RPC;

@CheckThreadSafety
public class LdtpStateMachine implements StateMachine {
    private static final String RAFT_COLUMN_FAMILY = "RAFT";
    private static final String META_INFO_COLUMN = "metaInfo";
    private static final String VOTE_FOR = "voteFor";
    private static final byte[] MEMBERS_KEY = G.I.toBytes("clusterMembers");
    private static final byte[] CURRENT_TERM = G.I.toBytes("currentTerm");

    // TODO: 能不能改成不是静态变量？
    private static LdtpEngineExecutor engineExecutor;
    private static RaftServer raftServer;

    private final Table raftMetaTable;

    public LdtpStateMachine() {
        Configuration config = Configuration.getInstance();
        LsmTreeOptions options = new LsmTreeOptions(config.raftMetaDir());
        raftMetaTable = new LynxDbTable(options);
    }

    public static void engineExecutor(LdtpEngineExecutor executor) {
        engineExecutor = executor;
    }

    public static void raftServer(RaftServer server) {
        raftServer = server;
    }

    @Override
    public synchronized void apply(List<ClientRequest> requests) {
        for(ClientRequest request : requests) {
            if(engineExecutor == null) {
                throw new RuntimeException();
            }

            SelectionKey selectionKey = request.selectionKey();
            int serial = request.serial();

            ByteBuffer buffer = ByteBuffer.wrap(request.data());
            byte type = buffer.get();

            switch (type) {
                case LDTP_METHOD -> engineExecutor.offerInterruptibly(request);
                case RAFT_RPC -> handleRaftRpc(selectionKey, serial, buffer);
                default -> throw new RuntimeException();
            }
        }
    }

    @CheckThreadSafety
    @Override
    public synchronized List<ServerNode> clusterMembers() {
        byte[] value = raftMetaTable.find(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN
        );
        return ServerNode.parseNodeList(value);
    }

    @Override
    public synchronized void addClusterMember(ServerNode member) {
        byte[] value = raftMetaTable.find(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN
        );

        List<ServerNode> members = ServerNode.parseNodeList(value);

        HashSet<ServerNode> memberSet = new HashSet<>(members);
        memberSet.add(member);

        byte[] newValue = ServerNode.nodesToBytes(memberSet);

        raftMetaTable.insert(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN,
                newValue,
                -1
        );
    }

    @Override
    public synchronized void addClusterMembers(List<ServerNode> members) {
        byte[] value = ServerNode.nodesToBytes(members);

        raftMetaTable.insert(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN,
                value,
                -1
        );
    }

    @CheckThreadSafety
    @Override
    public synchronized int currentTerm() {
        byte[] val = raftMetaTable.find(
                CURRENT_TERM,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN
        );

        if(val == null) {
            throw new RuntimeException();
        }

        return ArrayUtils.toInt(val);
    }

    @Override
    public synchronized void currentTerm(int term) {
        raftMetaTable.insert(
                CURRENT_TERM,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN,
                BufferUtils.toBytes(term),
                -1
        );
    }

    @Override
    public synchronized ServerNode voteFor(int term) {
        byte[] key = BufferUtils.toBytes(term);

        byte[] value = raftMetaTable.find(
                key,
                RAFT_COLUMN_FAMILY,
                VOTE_FOR
        );

        return value == null ? null : ServerNode.from(G.I.toString(value));
    }

    @Override
    public synchronized boolean voteForIfNull(int term, ServerNode node) {
        if(node == null) {
            throw new RuntimeException();
        }

        byte[] key = BufferUtils.toBytes(term);
        byte[] findValue = raftMetaTable.find(
                key,
                RAFT_COLUMN_FAMILY,
                VOTE_FOR
        );

        byte[] value = G.I.toBytes(node.toString());

        if(findValue != null && !Arrays.equals(findValue, value)) {
            return false;
        }

        raftMetaTable.insert(
                key,
                RAFT_COLUMN_FAMILY,
                VOTE_FOR,
                value,
                -1
        );

        return true;
    }

    private void handleRaftRpc(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        byte method = buffer.get();

        switch (method) {
            case JOIN_CLUSTER -> handleJoinCluster(selectionKey, serial, buffer);
            case LEAVE_CLUSTER -> handleLeaveCluster(selectionKey, serial, buffer);
            default -> throw new RuntimeException();
        }
    }

    private void handleJoinCluster(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        String node = BufferUtils.getRemainingString(buffer);
        ServerNode newMember = ServerNode.from(node);
        addClusterMember(newMember);

        JoinClusterResult result = new JoinClusterResult(LdtpCode.TRUE);
        WritableSocketResponse response = new WritableSocketResponse(
                selectionKey,
                serial,
                result.toBuffers()
        );
        raftServer.offerInterruptibly(response);
    }

    private void handleLeaveCluster(SelectionKey selectionKey, int serial, ByteBuffer buffer) {
        throw new UnsupportedOperationException();
    }
}
