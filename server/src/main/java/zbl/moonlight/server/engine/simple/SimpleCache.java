package zbl.moonlight.server.engine.simple;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.Parser;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.utils.ByteArrayUtils;
import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequest;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.engine.Engine;
import zbl.moonlight.server.raft.RaftNode;
import zbl.moonlight.server.raft.RaftRole;
import zbl.moonlight.server.raft.log.RaftLogEntry;
import zbl.moonlight.server.raft.schema.AppendEntriesArgs;
import zbl.moonlight.server.raft.schema.AppendEntriesArgsSchema;
import zbl.moonlight.server.raft.schema.RequestVoteArgs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;

public class SimpleCache extends Engine {
    private final Logger logger = LogManager.getLogger("SimpleCache");

    private final SimpleLRU<String, byte[]> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(MdtpServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.GET)
    public NioWriter doGet(MdtpRequest request) {
        byte[] value = cache.get(new String(request.key()));
        byte status = value == null ? ResponseStatus.VALUE_NOT_EXIST : ResponseStatus.VALUE_EXIST;
        return buildMdtpResponseEvent(request.selectionKey(), status, request.serial(), value);
    }

    @MethodMapping(MdtpMethod.SET)
    public NioWriter doSet(MdtpRequest request) {
        cache.put(new String(request.key()), request.value());
        return buildMdtpResponseEvent(request.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE,
                request.serial());
    }

    @MethodMapping(MdtpMethod.DELETE)
    public NioWriter doDelete(MdtpRequest request) {
        cache.remove(new String(request.key()));
        return buildMdtpResponseEvent(request.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE,
                request.serial());
    }

    @MethodMapping(MdtpMethod.PING)
    public NioWriter doPing(MdtpRequest request) {
        return buildMdtpResponseEvent(request.selectionKey(), ResponseStatus.PONG, request.serial());
    }

    @MethodMapping(MdtpMethod.REQUEST_VOTE)
    public NioWriter doRequestVote(MdtpRequest request) throws IOException {
        RaftNode node = parseRaftNode(request.key());
        byte[] value = request.value();
        RequestVoteArgs args = new RequestVoteArgs(value);

        int currentTerm = raftState.getCurrentTerm();
        RaftNode votedFor = raftState.getVotedFor();

        logger.debug("Remote(Request) term is: {}, Local(Current) term is: {}."
                , args.term(), raftState.getCurrentTerm());

        if(args.term() < currentTerm) {
            return buildMdtpResponseEvent(request.selectionKey(),
                    ResponseStatus.DID_NOT_GET_VOTE, request.serial(),
                    ByteArrayUtils.fromInt(currentTerm));
        } else if(args.term() > currentTerm) {
            raftState.setCurrentTerm(args.term());
            raftState.setRaftRole(RaftRole.Follower);
        }

        if((votedFor == null || votedFor.equals(node))
                && args.lastLogTerm() >= raftState.lastLogTerm()
                && args.lastLogIndex() >= raftState.lastApplied()) {
            logger.info("Voted for node: {}", node);

            raftState.setVotedFor(node);
            return buildMdtpResponseEvent(request.selectionKey(),
                    ResponseStatus.GET_VOTE, request.serial(),
                    ByteArrayUtils.fromInt(raftState.getCurrentTerm()));
        }

        return buildMdtpResponseEvent(request.selectionKey(),
                ResponseStatus.DID_NOT_GET_VOTE, request.serial());
    }

    @MethodMapping(MdtpMethod.APPEND_ENTRIES)
    public NioWriter doAppendEntries(MdtpRequest request) {
        byte[] value = request.value();

        if(value.length == 0) {
            logger.info("Received heartbeat from leader: {}",
                    request.selectionKey().attachment());
        } else {
            AppendEntriesArgs args = new AppendEntriesArgs(value);
            for(RaftLogEntry entry : args.entries()) {
            }
        }

        return buildMdtpResponseEvent(request.selectionKey(),
                ResponseStatus.APPEND_ENTRIES_SUCCESS, request.serial());
    }

    private RaftNode parseRaftNode(byte[] bytes) {
        String key = new String(bytes);
        String[] strings = key.split(":");
        return new RaftNode(strings[0], Integer.parseInt(strings[1]));
    }
}
