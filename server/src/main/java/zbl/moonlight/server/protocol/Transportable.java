package zbl.moonlight.server.protocol;

import java.io.IOException;
import java.io.Serializable;
import java.nio.channels.SocketChannel;

public interface Transportable extends Serializable {
    /* 从SocketChannel中读数据 */
    void read(SocketChannel socketChannel) throws IOException;
    /* 写数据到SocketChannel中 */
    void write(SocketChannel socketChannel) throws IOException;
    /* 是否读数据完成 */
    boolean isReadCompleted();
    /* 是否写数据完成 */
    boolean isWriteCompleted();
}
