module lynxdb.client {
    requires transitive lynxdb.server;

    exports com.bailizhang.lynxdb.client;
    exports com.bailizhang.lynxdb.client.annotation;
    exports com.bailizhang.lynxdb.client.message;
}