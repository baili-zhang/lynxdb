package com.bailizhang.lynxdb.socket.interfaces;

import java.io.IOException;

public interface Readable {
    /** 读数据 */
    void read() throws IOException;
    /** 是否读数据完成 */
    boolean isReadCompleted();
}
