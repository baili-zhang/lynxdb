package zbl.moonlight.server.storage.core;

import zbl.moonlight.core.socket.response.SocketResponse;

import java.nio.channels.SelectionKey;

public class ResultSet extends SocketResponse {
    public static final byte SUCCESS = (byte) 0x01;
    public static final byte FAILURE = (byte) 0x02;

    private static final byte[] EMPTY_VALUE = new byte[0];
    private static final String EMPTY_MESSAGE = "";

    private byte[] value = EMPTY_VALUE;
    private byte code = SUCCESS;
    private String message = EMPTY_MESSAGE;

    public ResultSet(SelectionKey selectionKey) {
        super(selectionKey);
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public void setCode(byte code) {
        this.code = code;
    }

    public void setMessage(String msg) {
        message = msg;
    }

    @Override
    public byte[] toBytes() {
        return value;
    }
}
