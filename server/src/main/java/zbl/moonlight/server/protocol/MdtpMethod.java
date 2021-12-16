package zbl.moonlight.server.protocol;

public class MdtpMethod {
    public static final byte SET = (byte) 0x01;
    public static final byte GET = (byte) 0x02;
    public static final byte UPDATE = (byte) 0x03;
    public static final byte DELETE = (byte) 0x04;
    public static final byte EXIT = (byte) 0x05;

    public static String getMethodName(byte code) {
        switch (code) {
            case SET:
                return "set";
            case GET:
                return "get";
            case UPDATE:
                return "update";
            case DELETE:
                return "delete";
            case EXIT:
                return "exit";
            default:
                return "unknown method";
        }
    }
}
