package zbl.moonlight.core.protocol;

import java.nio.ByteBuffer;
import java.util.HashMap;

public interface Parsable {
    /** 将数据解析成map */
    HashMap<String, byte[]> parse(ByteBuffer data);
}
