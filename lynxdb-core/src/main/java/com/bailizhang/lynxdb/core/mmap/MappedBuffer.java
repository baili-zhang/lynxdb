package com.bailizhang.lynxdb.core.mmap;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;

import java.lang.ref.SoftReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MappedBuffer {
    private final Path filePath;
    private final long position;
    private final int length;

    private FileChannel channel;

    // 内存溢出前，则会被回收
    private SoftReference<MappedByteBuffer> softBuffer;

    public MappedBuffer(Path filePath, long position, int length) {
        this.filePath = filePath;
        this.position = position;
        this.length = length;

        channel = FileChannelUtils.open(
                filePath,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        MappedByteBuffer mappedBuffer = FileChannelUtils.map(
                channel,
                FileChannel.MapMode.READ_WRITE,
                position,
                length
        );

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
                        position,
                        length
                );

                softBuffer = new SoftReference<>(mappedBuffer);
            }
        }

        return mappedBuffer.rewind();
    }

    public int length() {
        return length;
    }

    public void force() {
        getBuffer().force();
    }
}
