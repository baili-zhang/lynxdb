module lynxdb.test.socket {
    requires lynxdb.core;
    requires lynxdb.timewheel;
    requires lynxdb.ldtp;
    requires lynxdb.socket;

    requires org.slf4j;
    requires org.junit.jupiter.api;

    opens com.bailizhang.lynxdb.test.socket.server to org.junit.platform.commons;
}