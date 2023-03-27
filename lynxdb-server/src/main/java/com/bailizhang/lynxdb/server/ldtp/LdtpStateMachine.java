package com.bailizhang.lynxdb.server.ldtp;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.lsmtree.LynxDbLsmTree;
import com.bailizhang.lynxdb.lsmtree.Table;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.raft.common.StateMachine;
import com.bailizhang.lynxdb.server.context.Configuration;
import com.bailizhang.lynxdb.socket.client.ServerNode;

import java.nio.ByteBuffer;
import java.util.List;

import static com.bailizhang.lynxdb.ldtp.annotations.LdtpMethod.CLUSTER_MEMBER_CHANGE;

public class LdtpStateMachine implements StateMachine {
    private static final String RAFT_COLUMN_FAMILY = "RAFT";
    private static final String MEMBERS_COLUMN = "members";
    private static final byte[] MEMBERS_KEY = G.I.toBytes("clusterMembers");

    private final Table clusterTable;

    public LdtpStateMachine() {
        Configuration config = Configuration.getInstance();
        LsmTreeOptions options = new LsmTreeOptions(config.raftDir());
        clusterTable = new LynxDbLsmTree(options);
    }

    @Override
    public void apply(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte flag = buffer.get();

        if (flag == CLUSTER_MEMBER_CHANGE) {
            byte[] members = BufferUtils.getRemaining(buffer);
            clusterTable.insert(
                    MEMBERS_KEY,
                    RAFT_COLUMN_FAMILY,
                    MEMBERS_COLUMN,
                    members
            );
        } else {
            throw new UnsupportedOperationException();
        }
    }

    @Override
    public List<ServerNode> clusterMembers() {
        byte[] value = clusterTable.find(MEMBERS_KEY, RAFT_COLUMN_FAMILY, MEMBERS_COLUMN);
        return ServerNode.parseNodeList(value);
    }
}
