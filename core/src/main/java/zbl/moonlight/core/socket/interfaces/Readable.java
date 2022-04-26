package zbl.moonlight.core.socket.interfaces;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public interface Readable {
    /** 读数据 */
    void read() throws IOException;
    /** 是否读数据完成 */
    boolean isReadCompleted();
}
