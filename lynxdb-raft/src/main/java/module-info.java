module lynxdb.raft {
    uses com.bailizhang.lynxdb.raft.spi.StateMachine;
    uses com.bailizhang.lynxdb.raft.spi.RaftConfiguration;

    requires static lynxdb.core;
    requires static lynxdb.socket;
    requires static lynxdb.timewheel;
    requires lynxdb.ldtp;

    requires org.slf4j;

    exports com.bailizhang.lynxdb.raft.client;
    exports com.bailizhang.lynxdb.raft.spi;
    exports com.bailizhang.lynxdb.raft.request;
    exports com.bailizhang.lynxdb.raft.result;
    exports com.bailizhang.lynxdb.raft.server;
    exports com.bailizhang.lynxdb.raft.core;
}