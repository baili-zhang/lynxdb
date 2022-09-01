package com.bailizhang.lynxdb.socket.interfaces;

import java.io.IOException;

public interface Writable {
    /** 写数据 */
    void write() throws IOException;
    /** 是否写数据完成 */
    boolean isWriteCompleted();
}
