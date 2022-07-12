package zbl.moonlight.storage.core;

import zbl.moonlight.storage.query.QueryTuple;

import java.util.List;

public class ResultSet {
    public static final byte SUCCESS = (byte) 0x01;
    public static final byte FAILURE = (byte) 0x02;

    public static final String EMPTY_MESSAGE = "";

    private byte code = SUCCESS;
    private String message = EMPTY_MESSAGE;

    private List<QueryTuple> result;

    public void setCode(byte code) {
        this.code = code;
    }

    public void setMessage(String msg) {
        message = msg;
    }

    public void setResult(List<QueryTuple> result) {
        this.result = result;
    }

    public byte code() {
        return code;
    }

    public String message() {
        return message;
    }

    public List<QueryTuple> result() {
        return result;
    }
}
