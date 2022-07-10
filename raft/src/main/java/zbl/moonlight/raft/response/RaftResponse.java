package zbl.moonlight.raft.response;

import zbl.moonlight.core.utils.NumberUtils;
import zbl.moonlight.socket.client.ServerNode;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface RaftResponse {
    byte REQUEST_VOTE_SUCCESS = (byte) 0x01;
    byte REQUEST_VOTE_FAILURE = (byte) 0x02;
    byte APPEND_ENTRIES_SUCCESS = (byte) 0x03;
    byte APPEND_ENTRIES_FAILURE = (byte) 0x04;

    byte CLIENT_REQUEST_SUCCESS = (byte) 0x05;
    /* 解析错误，则客户端请求失败 */
    byte CLIENT_REQUEST_FAILURE = (byte) 0x06;

    static byte[] requestVoteSuccess(int term, ServerNode currentNode) {
        byte[] hostBytes = currentNode.host().getBytes(StandardCharsets.UTF_8);
        int hostLength = hostBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH * 3 + hostLength + 1);
        return buffer.put(REQUEST_VOTE_SUCCESS).putInt(term).putInt(hostLength).put(hostBytes)
                .putInt(currentNode.port()).array();
    }

    static byte[] requestVoteFailure(int term, ServerNode currentNode) {
        byte[] hostBytes = currentNode.host().getBytes(StandardCharsets.UTF_8);
        int hostLength = hostBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH * 3 + hostLength + 1);
        return buffer.put(REQUEST_VOTE_FAILURE).putInt(term).putInt(hostLength).put(hostBytes)
                .putInt(currentNode.port()).array();
    }

    static byte[] appendEntriesSuccess(int term, ServerNode node, int matchIndex) {
        byte[] hostBytes = node.host().getBytes(StandardCharsets.UTF_8);
        int hostLength = hostBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH * 4 + hostLength + 1);
        return buffer.put(APPEND_ENTRIES_SUCCESS).putInt(term).putInt(hostLength).put(hostBytes)
                .putInt(node.port()).putInt(matchIndex).array();
    }

    static byte[] appendEntriesFailure(int term, ServerNode node) {
        byte[] hostBytes = node.host().getBytes(StandardCharsets.UTF_8);
        int hostLength = hostBytes.length;
        ByteBuffer buffer = ByteBuffer.allocate(NumberUtils.INT_LENGTH * 3 + hostLength + 1);
        return buffer.put(APPEND_ENTRIES_FAILURE).putInt(term).putInt(hostLength).put(hostBytes)
                .putInt(node.port()).array();
    }

    static byte[] clientRequestSuccess(byte[] commandResult) {
        int len = commandResult.length + 1;
        ByteBuffer buffer = ByteBuffer.allocate(len);
        return buffer.put(CLIENT_REQUEST_SUCCESS).put(commandResult).array();
    }

    static byte[] clientRequestSuccessWithoutResult() {
        return clientRequestSuccess(new byte[0]);
    }

    static byte[] clientRequestFailure() {
        return new byte[]{CLIENT_REQUEST_FAILURE};
    }
}
