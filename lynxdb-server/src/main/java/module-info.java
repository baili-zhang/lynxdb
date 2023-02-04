module lynxdb.server {
    requires lynxdb.core;
    requires lynxdb.socket;
    requires lynxdb.lsmtree;
    requires lynxdb.raft;
    requires lynxdb.timewheel;
    requires lynxdb.ldtp;

    exports com.bailizhang.lynxdb.server.mode;
}