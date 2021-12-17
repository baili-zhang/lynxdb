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
        int index = srcOffset / src.chunkSize;
        int startPosition = srcOffset % src.chunkSize;
        ByteBuffer dstChunk = ByteBuffer.allocateDirect(chunkSize);
        bufferList.add(dstChunk);

        while (length > 0) {
            ByteBuffer srcChunk = src.bufferList.get(index);

            for(int i = startPosition; (i < srcChunk.limit()) && (length > 0); i ++, length --) {
                dstChunk.put(srcChunk.get(i));
                if(dstChunk.position() == dstChunk.limit()) {
                    dstChunk = ByteBuffer.allocateDirect(chunkSize);
                    bufferList.add(dstChunk);
                }
            }

            index ++;
            startPosition = 0;
        }
    }

    public void copyTo(ByteBuffer dst, int srcOffset, int length) {
        int index = srcOffset / chunkSize;
        int startPosition = srcOffset % chunkSize;

        while (length > 0) {
            ByteBuffer srcChunk = bufferList.get(index);

            for(int i = startPosition; (i < srcChunk.limit()) && (length > 0); i ++, length --) {
                dst.put(srcChunk.get(i));
            }

            index ++;
            startPosition = 0;
        }
    }

    public int getChunkSize() {
        return chunkSize;
    }

    public ByteBuffer get(int index) {
        return bufferList.get(index);
    }

    public ByteBuffer[] array() {
        return bufferList.stream().toArray(ByteBuffer[]::new);
    }

    private boolean isEmpty() {
        return bufferList.size() == 0;
    }
}
