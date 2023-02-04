module lynxdb.socket {
    requires transitive lynxdb.core;

    exports com.bailizhang.lynxdb.socket.client;
    exports com.bailizhang.lynxdb.socket.code;
    exports com.bailizhang.lynxdb.socket.common;
    exports com.bailizhang.lynxdb.socket.interfaces;
    exports com.bailizhang.lynxdb.socket.request;
    exports com.bailizhang.lynxdb.socket.response;
    exports com.bailizhang.lynxdb.socket.result;
    exports com.bailizhang.lynxdb.socket.server;
}