package com.bailizhang.lynxdb.core.utils;

import com.bailizhang.lynxdb.core.common.G;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;

public interface FileChannelUtils {
    String MMAP = "MMap";

    static FileChannel open(String filePath, OpenOption... options) {
        return open(Path.of(filePath), options);
    }

    static FileChannel open(Path filePath, OpenOption... options) {
        try {
            return FileChannel.open(filePath, options);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static byte read(FileChannel channel, int idx) {
        ByteBuffer buffer = ByteBuffer.allocate(PrimitiveTypeUtils.BYTE_LENGTH);

        try {
            channel.read(buffer, idx);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return buffer.rewind().get();
    }

    static byte[] read(FileChannel channel, int dataBegin, int length) {
        ByteBuffer buffer = ByteBuffer.allocate(length);

        try {
            channel.read(buffer, dataBegin);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return buffer.array();
    }

    static int readInt(FileChannel channel, int dataBegin) {
        ByteBuffer buffer = BufferUtils.intByteBuffer();

        try {
            channel.read(buffer, dataBegin);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return buffer.rewind().getInt();
    }

    static void write(FileChannel channel, byte data, int idx) {
        ByteBuffer buffer = ByteBuffer.allocate(PrimitiveTypeUtils.BYTE_LENGTH);
        buffer.put(data).rewind();

        write(channel, buffer, idx);
    }

    static void write(FileChannel channel, byte[] data, int idx) {
        write(channel, ByteBuffer.wrap(data), idx);
    }

    static void write(FileChannel channel, ByteBuffer buffer, int idx) {
        try {
            channel.write(buffer, idx);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    static void write(FileChannel channel, byte[] data) {
        try {
            channel.write(ByteBuffer.wrap(data));
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    static void write(FileChannel channel, ByteBuffer buffer) {
        try {
            channel.write(buffer);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    static long size(FileChannel channel) {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static void force(FileChannel channel, boolean metaData) {
        try {
            channel.force(metaData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static MappedByteBuffer map(FileChannel channel, FileChannel.MapMode mode, long position, long size) {
        G.I.incrementRecord(MMAP, 1);

        try {
            return channel.map(mode, position, size);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
