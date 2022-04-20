package zbl.moonlight.server.mdtp;

import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

public class MdtpMethod {
    public static final byte SET = (byte) 0x01;
    public static final byte GET = (byte) 0x02;
    public static final byte UPDATE = (byte) 0x03;
    public static final byte DELETE = (byte) 0x04;
    public static final byte EXIT = (byte) 0x05;
    public static final byte SYSTEM = (byte) 0x06;
    public static final byte CLUSTER = (byte) 0x07;
    public static final byte PING = (byte) 0x08;
    /* Raft相关 */
    public static final byte APPEND_ENTRIES = (byte) 0x09;
    public static final byte REQUEST_VOTE = (byte) 0x0a;
    /* 重定向的方法 */
    public static final byte REDIRECT_SET = (byte) 0x0b;
    public static final byte REDIRECT_DELETE = (byte) 0x0c;

    public static final ConcurrentHashMap<Byte, String> methodNamesMap = new ConcurrentHashMap<>();

    static {
        try {
            Field[] fields = MdtpMethod.class.getDeclaredFields();
            for (Field field : fields) {
                if(field.getType().equals(byte.class)) {
                    methodNamesMap.put(field.getByte(MdtpMethod.class), field.getName());
                }
            }
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }
    }

    public static String getMethodName(byte code) {
        return methodNamesMap.get(code);
    }
}
