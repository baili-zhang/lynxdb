module lynxdb.client {
    requires lynxdb.core;
    requires lynxdb.socket;
    requires lynxdb.lsmtree;
    requires lynxdb.server;

    exports com.bailizhang.lynxdb.client;
    exports com.bailizhang.lynxdb.client.annotation;
    exports com.bailizhang.lynxdb.client.message;
}