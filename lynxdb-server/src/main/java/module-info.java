module lynxdb.server {
    requires lynxdb.core;
    requires lynxdb.socket;
    requires lynxdb.lsmtree;
    requires lynxdb.raft;
    requires lynxdb.timewheel;

    exports com.bailizhang.lynxdb.server.annotations;
    exports com.bailizhang.lynxdb.server.mode;
    exports com.bailizhang.lynxdb.server.engine.message;
    exports com.bailizhang.lynxdb.server.engine.affect;
}