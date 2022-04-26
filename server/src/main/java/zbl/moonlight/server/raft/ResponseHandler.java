package zbl.moonlight.server.raft;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.mdtp.MdtpRequest;
import zbl.moonlight.server.mdtp.MdtpResponse;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;

public class ResponseHandler {
    private static final Logger logger = LogManager.getLogger("ResponseHandler");

    private static final RaftState raftState;
    private static final Configuration config;
    private static final EventBus eventBus;
    private static final HashSet<RaftNode> votedNodes;

    static {
        MdtpServerContext context = MdtpServerContext.getInstance();
        raftState = context.getRaftState();
        config = context.getConfiguration();
        eventBus = context.getEventBus();
        votedNodes = raftState.getVotedNodes();
    }

    public static void handle(NioReader reader, RaftNode raftNode, ConcurrentLinkedQueue<MdtpRequest> pendingRequests) {
        MdtpResponse response = new MdtpResponse(reader);
        switch (response.status()) {
            case ResponseStatus.GET_VOTE -> handleGetVote(response, raftNode);
            case ResponseStatus.DID_NOT_GET_VOTE -> handleDidNotGetVote(raftNode);
            case ResponseStatus.APPEND_ENTRIES_SUCCESS -> handleAppendEntriesSuccess(response, raftNode, pendingRequests);
            case ResponseStatus.APPEND_ENTRIES_FAIL -> handleAppendEntriesFail(response, raftNode);
        }
    }

    /**
     * 处理请求投票成功
     */
    public static void handleGetVote(MdtpResponse response, RaftNode raftNode) {
        int term = ByteArrayUtils.toInt(response.value());
        if(term == raftState.getCurrentTerm()) {
            synchronized (votedNodes) {
                votedNodes.add(raftNode);
            }

            if(votedNodes.size() >= (raftState.getRaftNodes().size() >> 1) + 1) {
                raftState.setRaftRole(RaftRole.Leader);
                logger.info("Set raft role to [RaftRole.Leader].");
            }
        }
    }

    /**
     * 处理请求投票失败
     */
    public static void handleDidNotGetVote(RaftNode raftNode) {
        logger.info("Did not get vote from {}.", raftNode);
    }

    /**
     * 处理增加日志条目成功
     */
    public static void handleAppendEntriesSuccess(MdtpResponse response, RaftNode raftNode,
                                                  ConcurrentLinkedQueue<MdtpRequest> pendingRequests) {
        eventBus.offer(new Event(EventType.CLUSTER_RESPONSE, pendingRequests.poll()));
    }

    /**
     * 处理增加日志条目失败
     */
    public static void handleAppendEntriesFail(MdtpResponse response, RaftNode raftNode) {

    }
}
