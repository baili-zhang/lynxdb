package zbl.moonlight.server.protocol;

public class ResponseCode {
    public static final byte VALUE_EXIST = (byte) 0x01;
    public static final byte VALUE_NOT_EXIST = (byte) 0x02;
    public static final byte SUCCESS_NO_VALUE = (byte) 0x03;

    public static String getCodeName(byte code) {
        switch (code) {
            case VALUE_EXIST:
                return "VALUE_EXIST";
            case VALUE_NOT_EXIST:
                return "VALUE_NOT_EXIST";
            case SUCCESS_NO_VALUE:
                return "SUCCESS_NO_VALUE";
        }
        return "INVALID_CODE";
    }
}
