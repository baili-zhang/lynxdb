module lynxdb.raft {
    requires static lynxdb.core;
    requires static lynxdb.socket;

    exports com.bailizhang.lynxdb.raft.client;
    exports com.bailizhang.lynxdb.raft.common;
    exports com.bailizhang.lynxdb.raft.request;
    exports com.bailizhang.lynxdb.raft.result;
    exports com.bailizhang.lynxdb.raft.server;
    exports com.bailizhang.lynxdb.raft.state;
    exports com.bailizhang.lynxdb.raft.timeout;
}