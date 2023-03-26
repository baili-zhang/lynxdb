package com.bailizhang.lynxdb.core.mmap;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MappedBuffer {
    private final Path filePath;

    private final long begin;
    private final int offset;

    private FileChannel channel;

    private int position;
    private int limit;

    // 内存溢出前，则会被回收
    private SoftReference<MappedByteBuffer> softBuffer;

    public MappedBuffer(Path filePath, long begin, int offset) {
        this.filePath = filePath;
        this.begin = begin;
        this.offset = offset;

        channel = FileChannelUtils.open(
                filePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        MappedByteBuffer mappedBuffer = FileChannelUtils.map(
                channel,
                FileChannel.MapMode.READ_WRITE,
                begin,
                offset
        );

        saveSnapshot(mappedBuffer);

        softBuffer = new SoftReference<>(mappedBuffer);
    }

    public MappedByteBuffer getBuffer() {
        MappedByteBuffer mappedBuffer = softBuffer.get();

        while (mappedBuffer == null) {
            if(softBuffer.refersTo(null)) {
                if(!channel.isOpen()) {
                    channel = FileChannelUtils.open(
                            filePath,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE
                    );
                }

                mappedBuffer = FileChannelUtils.map(
                        channel,
                        FileChannel.MapMode.READ_WRITE,
                        begin,
                        offset
                );

                mappedBuffer.position(position);
                mappedBuffer.limit(limit);

                softBuffer = new SoftReference<>(mappedBuffer);
            }
        }

        return mappedBuffer;
    }

    public void saveSnapshot(MappedByteBuffer buffer) {
        position = buffer.position();
        limit = buffer.limit();
    }

    public int length() {
        return offset;
    }

    public void force() {
        getBuffer().force();
    }
}
