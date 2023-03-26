module lynxdb.raft {
    uses com.bailizhang.lynxdb.raft.common.StateMachine;
    uses com.bailizhang.lynxdb.raft.common.RaftConfiguration;

    requires static lynxdb.core;
    requires static lynxdb.socket;
    requires static lynxdb.timewheel;

    requires org.slf4j;

    exports com.bailizhang.lynxdb.raft.client;
    exports com.bailizhang.lynxdb.raft.common;
    exports com.bailizhang.lynxdb.raft.request;
    exports com.bailizhang.lynxdb.raft.result;
    exports com.bailizhang.lynxdb.raft.server;
    exports com.bailizhang.lynxdb.raft.core;
}