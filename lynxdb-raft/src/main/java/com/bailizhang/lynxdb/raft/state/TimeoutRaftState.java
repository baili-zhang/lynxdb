package com.bailizhang.lynxdb.raft.state;

import com.bailizhang.lynxdb.core.timeout.Timeout;
import com.bailizhang.lynxdb.raft.common.RaftConfiguration;
import com.bailizhang.lynxdb.raft.timeout.ElectionTask;
import com.bailizhang.lynxdb.raft.timeout.HeartbeatTask;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.atomic.AtomicInteger;

public class TimeoutRaftState extends LogRaftState {
    private static final Logger logger = LogManager.getLogger("TimeoutRaftState");

    private static final String HEARTBEAT_TIMEOUT_NAME = "HeartBeat_Timeout";
    private static final String ELECTION_TIMEOUT_NAME = "Election_Timeout";

    private static final int HEARTBEAT_INTERVAL_MILLIS = 80;
    private static final int ELECTION_MIN_INTERVAL_MILLIS = 150;
    private static final int ELECTION_MAX_INTERVAL_MILLIS = 300;

    private final Timeout heartbeat;
    private final Timeout election;

    private final Thread heartbeatThread;
    private final Thread electionThread;

    protected final AtomicInteger electionTimeoutTimes = new AtomicInteger(0);

    public TimeoutRaftState() {
        final int ELECTION_INTERVAL_MILLIS = ((int) (Math.random() *
                (ELECTION_MAX_INTERVAL_MILLIS - ELECTION_MIN_INTERVAL_MILLIS)))
                + ELECTION_MIN_INTERVAL_MILLIS;

        heartbeat = new Timeout(new HeartbeatTask(), HEARTBEAT_INTERVAL_MILLIS);
        election = new Timeout(new ElectionTask(electionTimeoutTimes), ELECTION_INTERVAL_MILLIS);

        heartbeatThread = new Thread(heartbeat, HEARTBEAT_TIMEOUT_NAME);
        electionThread = new Thread(election, ELECTION_TIMEOUT_NAME);
    }

    public void startTimeout() {
        String electionMode = raftConfiguration.electionMode();

        if (isLeader() || RaftConfiguration.LEADER.equals(electionMode)
                || RaftConfiguration.CANDIDATE.equals(electionMode)) {

            heartbeatThread.start();
            electionThread.start();

            logger.info("Election Mode is [{}], raft role is: [{}]", electionMode, raftRole);
            return;
        }

        logger.info("Election Mode is [{}], Do not start Timeout.", electionMode);
    }

    public void resetElectionTimeout() {
        election.reset();
    }

    public void resetHeartbeatTimeout() {
        heartbeat.reset();
    }
}
