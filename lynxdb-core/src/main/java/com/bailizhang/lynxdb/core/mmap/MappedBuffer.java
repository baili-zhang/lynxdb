package com.bailizhang.lynxdb.core.mmap;

import com.bailizhang.lynxdb.core.utils.FileChannelUtils;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class MappedBuffer {
    private static final int THRESHOLD = 20;
    private static final List<MappedBuffer> mappedBuffers = new ArrayList<>();

    private final Path filePath;
    private final long position;
    private final int length;

    private int count = 0;

    private FileChannel channel;

    // 内存溢出前，则会被回收
    private SoftReference<MappedByteBuffer> softBuffer;
    // 触发 GC，则会被回收
    private WeakReference<MappedByteBuffer> weakBuffer;

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
        weakBuffer = new WeakReference<>(null);

        mappedBuffers.add(this);
    }

    public MappedByteBuffer getBuffer() {
        mappedBuffers.forEach(mappedBuffer -> {
            if(mappedBuffer == this) {
                count = 0;
                return;
            }

            mappedBuffer.check();
        });

        MappedByteBuffer mappedBuffer = null;

        while (mappedBuffer == null) {
            if(softBuffer.refersTo(null) && weakBuffer.refersTo(null)) {
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
                return mappedBuffer;
            }

            mappedBuffer = softBuffer.refersTo(null) ? weakBuffer.get() : softBuffer.get();
        }

        return mappedBuffer;
    }

    public int length() {
        return length;
    }

    public void force() {
        getBuffer().force();
    }

    private void check() {
        if(++ count > THRESHOLD && !softBuffer.refersTo(null)) {
            MappedByteBuffer mappedBuffer = softBuffer.get();
            softBuffer = new SoftReference<>(null);
            weakBuffer = new WeakReference<>(mappedBuffer);
        }
    }
}
