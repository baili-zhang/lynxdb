package zbl.moonlight.server.protocol.mdtp;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

public class ResponseStatus {
    public static final byte VALUE_EXIST = (byte) 0x01;
    public static final byte VALUE_NOT_EXIST = (byte) 0x02;
    public static final byte SUCCESS_NO_VALUE = (byte) 0x03;
    public static final byte ERROR = (byte) 0x04;
    public static final byte PONG = (byte) 0x05;

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
