package zbl.moonlight.server.engine.simple;

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
import zbl.moonlight.server.raft.RaftState;
import zbl.moonlight.server.raft.schema.RequestVoteArgs;

public class SimpleCache extends Engine {
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

        /* 解析候选人的Host和Port */
        String key = new String(request.key());
        String[] strings = key.split(":");
        String candidateHost = strings[0];
        int candidatePort = Integer.parseInt(strings[1]);

        byte[] value = request.value();

        RequestVoteArgs args = new RequestVoteArgs(value);

        RaftState raftState = MdtpServerContext.getInstance().getRaftState();
        RaftRole raftRole = raftState.getRaftRole();

        if(args.term() < raftState.getCurrentTerm()) {
            // false
        }

        RaftNode votedFor = raftState.getVotedFor();

        if(votedFor == null
                /* TODO:（添加条件）或者日志一样新 */
                || (votedFor.host().equals(candidateHost) && votedFor.port() == candidatePort)) {
            // true
        }

        return buildMdtpResponseEvent(reader.getSelectionKey(), ResponseStatus.PONG, request.serial());
    }
}
