package zbl.moonlight.server.mdtp;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

public class ResponseStatus {
    public static final byte VALUE_EXIST = (byte) 0x01;
    public static final byte VALUE_NOT_EXIST = (byte) 0x02;
    public static final byte SUCCESS_NO_VALUE = (byte) 0x03;
    public static final byte ERROR = (byte) 0x04;
    /* 检查服务器是否正常工作 */
    public static final byte PONG = (byte) 0x05;
    /* Raft投票请求相关 */
    public static final byte GET_VOTE = (byte) 0x06;
    public static final byte DID_NOT_GET_VOTE = (byte) 0x07;
    public static final byte APPEND_ENTRIES_SUCCESS = (byte) 0x08;
    public static final byte APPEND_ENTRIES_FAIL = (byte) 0x09;

    public static final ConcurrentHashMap<Byte, String> codeMap = new ConcurrentHashMap<>();

    static {
        try {
            Field[] fields = ResponseStatus.class.getDeclaredFields();
            for (Field field : fields) {
                if(field.getType().equals(byte.class)) {
                    codeMap.put(field.getByte(ResponseStatus.class), field.getName());
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static String getCodeName(byte code) {
        return codeMap.get(code);
    }
}
