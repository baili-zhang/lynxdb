package zbl.moonlight.core.protocol.common;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface Writable {
    /* 写数据到SocketChannel中 */
    void write(SocketChannel socketChannel) throws IOException;
    /* 是否写数据完成 */
    boolean isWriteCompleted();
}
