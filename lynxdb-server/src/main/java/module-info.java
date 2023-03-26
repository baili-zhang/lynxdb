module lynxdb.server {
    requires lynxdb.core;
    requires lynxdb.socket;
    requires lynxdb.lsmtree;
    requires lynxdb.raft;
    requires lynxdb.timewheel;
    requires lynxdb.ldtp;

    requires org.slf4j;

    provides com.bailizhang.lynxdb.raft.common.StateMachine
            with com.bailizhang.lynxdb.server.ldtp.LdtpStateMachine;
    provides com.bailizhang.lynxdb.raft.common.RaftConfiguration
            with com.bailizhang.lynxdb.server.context.LynxDbRaftConfiguration;

    opens com.bailizhang.lynxdb.server.context to lynxdb.core;
}