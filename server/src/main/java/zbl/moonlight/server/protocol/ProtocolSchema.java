package zbl.moonlight.server.protocol;

import java.nio.ByteBuffer;
import java.util.HashMap;

public interface ProtocolSchema {
    HashMap<String, byte[]> parse(ByteBuffer data);
}
