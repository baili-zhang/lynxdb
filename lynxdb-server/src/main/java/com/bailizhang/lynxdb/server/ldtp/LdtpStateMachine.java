package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.ByteArrayUtils;
import com.bailizhang.lynxdb.lsmtree.LynxDbLsmTree;
import com.bailizhang.lynxdb.lsmtree.Table;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.raft.core.ClientRequest;
import com.bailizhang.lynxdb.raft.spi.StateMachine;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.server.mode.LdtpEngineExecutor;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.util.HashSet;
import java.util.List;

public class LdtpStateMachine implements StateMachine {
    private static final String RAFT_COLUMN_FAMILY = "RAFT";
    private static final String META_INFO_COLUMN = "metaInfo";
    private static final byte[] MEMBERS_KEY = G.I.toBytes("clusterMembers");
    private static final byte[] CURRENT_TERM = G.I.toBytes("currentTerm");

    // TODO: 能不能改成不是静态变量？
    private static LdtpEngineExecutor engineExecutor;

    private final Table clusterTable;

    public LdtpStateMachine() {
        Configuration config = Configuration.getInstance();
        LsmTreeOptions options = new LsmTreeOptions(config.raftMetaDir());
        clusterTable = new LynxDbLsmTree(options);
    }

    public static void engineExecutor(LdtpEngineExecutor executor) {
        engineExecutor = executor;
    }

    @Override
    public void apply(List<ClientRequest> requests) {
        for(ClientRequest request : requests) {
            if(engineExecutor == null) {
                throw new RuntimeException();
            }

            engineExecutor.offerInterruptibly(request);
        }
    }

    @Override
    public List<ServerNode> clusterMembers() {
        byte[] value = clusterTable.find(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN
        );
        return ServerNode.parseNodeList(value);
    }

    @Override
    public void addClusterMember(ServerNode member) {
        byte[] value = clusterTable.find(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN
        );

        List<ServerNode> members = ServerNode.parseNodeList(value);

        HashSet<ServerNode> memberSet = new HashSet<>(members);
        memberSet.add(member);

        byte[] newValue = ServerNode.nodesToBytes(memberSet);

        clusterTable.insert(
                MEMBERS_KEY,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN,
                newValue
        );
    }

    @Override
    public int currentTerm() {
        byte[] val = clusterTable.find(
                CURRENT_TERM,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN
        );

        if(val == null) {
            throw new RuntimeException();
        }

        return ByteArrayUtils.toInt(val);
    }

    @Override
    public void currentTerm(int term) {
        clusterTable.insert(
                CURRENT_TERM,
                RAFT_COLUMN_FAMILY,
                META_INFO_COLUMN,
                BufferUtils.toBytes(term)
        );
    }
}
