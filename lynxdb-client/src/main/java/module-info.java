module lynxdb.client {
    requires lynxdb.core;
    requires lynxdb.socket;
    requires lynxdb.server;
    requires lynxdb.lsmtree;

    exports com.bailizhang.lynxdb.client;
    exports com.bailizhang.lynxdb.client.annotation;
    exports com.bailizhang.lynxdb.client.message;
}