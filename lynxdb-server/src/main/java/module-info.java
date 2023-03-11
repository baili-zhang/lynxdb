module lynxdb.server {
    requires lynxdb.core;
    requires lynxdb.socket;
    requires lynxdb.lsmtree;
    requires lynxdb.raft;
    requires lynxdb.timewheel;
    requires lynxdb.ldtp;

    requires org.slf4j;

    exports com.bailizhang.lynxdb.server.mode;
}