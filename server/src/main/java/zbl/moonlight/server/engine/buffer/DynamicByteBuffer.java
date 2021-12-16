package zbl.moonlight.server.engine.buffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DynamicByteBuffer {
    private List<ByteBuffer> bufferList = new ArrayList<>();
    private int chunkSize = 1024;

    public DynamicByteBuffer() {

    }

    public DynamicByteBuffer(int chunkSize) {
        if(chunkSize > 6) {
            this.chunkSize = chunkSize;
        }
    }

    public ByteBuffer last() {
        if(isEmpty() || isFull()) {
            bufferList.add(ByteBuffer.allocate(chunkSize));
        }
        return bufferList.get(bufferList.size() - 1);
    }

    public boolean isFull() {
        ByteBuffer last = bufferList.get(bufferList.size() - 1);
        return last.position() == last.limit();
    }

    public int size() {
        return chunkSize * (bufferList.size() - 1) + last().position();
    }

    public void copyFrom(DynamicByteBuffer src, int srcOffset, int length) {

    }

    public void copyTo(ByteBuffer dst, int srcOffset, int length) {

    }

    public ByteBuffer getFirst() {
        return bufferList.get(0);
    }

    private boolean isEmpty() {
        return bufferList.size() == 0;
    }
}
