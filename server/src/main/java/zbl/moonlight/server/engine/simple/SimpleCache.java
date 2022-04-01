package zbl.moonlight.server.engine.simple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequest;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.raft.RaftNode;
import zbl.moonlight.server.raft.RaftRole;
import zbl.moonlight.server.raft.schema.RequestVoteArgs;

public class SimpleCache extends Engine {
    private final Logger logger = LogManager.getLogger("SimpleCache");

    private final SimpleLRU<String, byte[]> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(MdtpServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.GET)
    public NioWriter doGet(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);
        byte[] value = cache.get(new String(request.key()));
        byte status = value == null ? ResponseStatus.VALUE_NOT_EXIST : ResponseStatus.VALUE_EXIST;
        return buildMdtpResponseEvent(reader.getSelectionKey(), status, request.serial(), value);
    }

    @MethodMapping(MdtpMethod.SET)
    public NioWriter doSet(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);
        cache.put(new String(request.key()), request.value());
        return buildMdtpResponseEvent(reader.getSelectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.DELETE)
    public NioWriter doDelete(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);
        cache.remove(new String(request.key()));
        return buildMdtpResponseEvent(reader.getSelectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.PING)
    public NioWriter doPing(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);
        return buildMdtpResponseEvent(reader.getSelectionKey(), ResponseStatus.PONG, request.serial());
    }

    @MethodMapping(MdtpMethod.REQUEST_VOTE)
    public NioWriter doRequestVote(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);

        RaftNode node = parseRaftNode(request.key());
        byte[] value = request.value();
        RequestVoteArgs args = new RequestVoteArgs(value);

        RaftRole raftRole = raftState.getRaftRole();
        RaftNode votedFor = raftState.getVotedFor();

        logger.debug("Remote(Request) term is: {}, Local(Current) term is: {}."
                , args.term(), raftState.getCurrentTerm());

        if(args.term() > raftState.getCurrentTerm()) {
            raftState.setCurrentTerm(args.term());
            raftState.setRaftRole(RaftRole.Follower);
            raftState.setVotedFor(node);
            return buildMdtpResponseEvent(reader.getSelectionKey(),
                    ResponseStatus.GET_VOTE, request.serial());
        }

        /* TODO:日志条目判断 */
        if(args.term() == raftState.getCurrentTerm()
                && votedFor == null) {
            raftState.setVotedFor(node);
            return buildMdtpResponseEvent(reader.getSelectionKey(),
                    ResponseStatus.GET_VOTE, request.serial());
        }

        return buildMdtpResponseEvent(reader.getSelectionKey(),
                ResponseStatus.DID_NOT_GET_VOTE, request.serial());
    }

    @MethodMapping(MdtpMethod.APPEND_ENTRIES)
    public NioWriter doAppendEntries(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);

        /* 重置定时器 */
        raftState.setTimeoutTimeMillis(System.currentTimeMillis());

        return buildMdtpResponseEvent(reader.getSelectionKey(),
                ResponseStatus.APPEND_ENTRIES_SUCCESS, request.serial());
    }

    private RaftNode parseRaftNode(byte[] bytes) {
        String key = new String(bytes);
        String[] strings = key.split(":");
        return new RaftNode(strings[0], Integer.parseInt(strings[1]));
    }
}
