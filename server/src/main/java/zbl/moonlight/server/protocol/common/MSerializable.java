package zbl.moonlight.server.protocol.common;

import java.nio.ByteBuffer;
import java.util.HashMap;

public interface MSerializable {
    /* 序列化操作 */
    ByteBuffer serialize(HashMap<String, byte[]> map);
}
