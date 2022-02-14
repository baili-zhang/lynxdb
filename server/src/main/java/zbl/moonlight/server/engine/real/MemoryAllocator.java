package zbl.moonlight.server.engine.real;

import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryAllocator {
    private static final int DEFAULT_CAPACITY = 4 * 1024 * 1024;
    private static ByteBuffer MEMO = null;
    private static int cursor = 0;

    private static final ReentrantLock lock = new ReentrantLock();

    public static void setMemoSize(int size) throws Exception {
        if(MEMO != null) {
            throw new Exception("can not set MEMO size, MEMO is not null.");
        }

        MEMO = ByteBuffer.allocateDirect(size);
    }

    public static void setMemoSize() throws Exception {
        setMemoSize(DEFAULT_CAPACITY);
    }

    public static ByteBuffer allocate (int size) throws Exception {
        if(MEMO == null) {
            throw new Exception("set MEMO size before allocate.");
        }

        // allocate memory from MEMO
        return null;
    }
}
