module lynxdb.client {
    requires lynxdb.core;
    requires lynxdb.lsmtree;
    requires lynxdb.socket;
    requires lynxdb.ldtp;

    exports com.bailizhang.lynxdb.client;
    exports com.bailizhang.lynxdb.client.annotation;
    exports com.bailizhang.lynxdb.client.message;
    exports com.bailizhang.lynxdb.client.connection;
}