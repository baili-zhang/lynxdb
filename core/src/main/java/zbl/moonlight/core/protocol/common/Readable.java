package zbl.moonlight.core.protocol.common;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface Readable {
    /* 从SocketChannel中读数据 */
    void read(SocketChannel socketChannel) throws IOException;
    /* 是否读数据完成 */
    boolean isReadCompleted();
}
