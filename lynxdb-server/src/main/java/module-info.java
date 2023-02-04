module lynxdb.server {
    requires transitive lynxdb.socket;
    requires transitive lynxdb.lsmtree;
    requires transitive lynxdb.raft;
    requires transitive lynxdb.timewheel;

    exports com.bailizhang.lynxdb.server.annotations;
    exports com.bailizhang.lynxdb.server.mode;
    exports com.bailizhang.lynxdb.server.engine.message;
    exports com.bailizhang.lynxdb.server.engine.affect;
}