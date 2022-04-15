package zbl.moonlight.server.raft;

import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.server.mdtp.MdtpResponse;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;

public class ResponseHandler {
    private static RaftState raftState = MdtpServerContext.getInstance().getRaftState();

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
