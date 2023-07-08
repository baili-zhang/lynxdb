module lynxdb.client {
    requires lynxdb.core;
    requires lynxdb.lsmtree;
    requires lynxdb.socket;
    requires lynxdb.ldtp;

    requires org.slf4j;

    exports com.bailizhang.lynxdb.client;
    exports com.bailizhang.lynxdb.client.annotation;
    exports com.bailizhang.lynxdb.client.connection;
}