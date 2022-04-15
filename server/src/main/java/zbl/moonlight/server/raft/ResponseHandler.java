package zbl.moonlight.server.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpResponse;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.util.concurrent.atomic.AtomicInteger;

public class ResponseHandler {
    private static final Logger logger = LogManager.getLogger("ResponseHandler");

    private static final RaftState raftState;
    private static final Configuration config;

    static {
        MdtpServerContext context = MdtpServerContext.getInstance();
        raftState = context.getRaftState();
        config = context.getConfiguration();
    }

    public static void handle(NioReader reader) {
        MdtpResponse response = new MdtpResponse(reader);
        switch (response.status()) {
            case ResponseStatus.GET_VOTE -> handleGetVote(response);
            case ResponseStatus.DID_NOT_GET_VOTE -> handleDidNotGetVote(response);
            case ResponseStatus.APPEND_ENTRIES_SUCCESS -> handleAppendEntriesSuccess(response);
            case ResponseStatus.APPEND_ENTRIES_FAIL -> handleAppendEntriesFail(response);
        }
    }

    /**
     * 处理请求投票成功
     */
    public static void handleGetVote(MdtpResponse response) {
        int term = ByteArrayUtils.toInt(response.value());
        if(term == raftState.getCurrentTerm()) {
            AtomicInteger voteCount = raftState.getVoteCount();

            int count = voteCount.get();
            while(!voteCount.compareAndSet(count, count + 1)) {
                count = voteCount.get();
            }

            if(voteCount.get() >= (config.getRaftNodes().size() >> 1) + 1) {
                raftState.setRaftRole(RaftRole.Leader);
                logger.info("Set raft role to [RaftRole.Leader].");
            }
        }
    }

    /**
     * 处理请求投票失败
     */
    public static void handleDidNotGetVote(MdtpResponse response) {

    }

    /**
     * 处理增加日志条目成功
     */
    public static void handleAppendEntriesSuccess(MdtpResponse response) {

    }

    /**
     * 处理增加日志条目失败
     */
    public static void handleAppendEntriesFail(MdtpResponse response) {

    }
}
