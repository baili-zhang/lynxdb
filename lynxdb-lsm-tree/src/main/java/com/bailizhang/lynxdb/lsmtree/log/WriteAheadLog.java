package com.bailizhang.lynxdb.lsmtree.log;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.Options;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

public class WriteAheadLog implements AutoCloseable {
    private static final String WAL_NAME = "WAL";

    private static final byte INSERT = (byte) 0x01;
    private static final byte DELETE = (byte) 0x02;

    private final Options options;
    private final FileChannel channel;
    private volatile int size;

    public WriteAheadLog(String dir, Options options) {
        this.options = options;
        String filename = WAL_NAME + FileUtils.LOG_SUFFIX;
        File file = FileUtils.createFileIfNotExisted(dir, filename);
        channel = FileUtils.open(file.getPath(), StandardOpenOption.WRITE);
    }

    public void appendInsert(byte[] key, byte[] column, long timestamp) {
        append(INSERT, key, column, timestamp);
    }

    public void appendDelete(byte[] key, byte[] column, long timestamp) {
        append(DELETE, key, column, timestamp);
    }

    public boolean isFull() {
        return size >= options.memTableSize();
    }

    @Override
    public void close() throws Exception {
        channel.close();
    }

    private void append(byte flag, byte[] key, byte[] column, long timestamp) {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(flag);
        bytesList.appendVarBytes(key);
        bytesList.appendVarBytes(column);
        bytesList.appendRawLong(timestamp);

        byte[] bytes = bytesList.toBytes();

        try {
            channel.write(ByteBuffer.wrap(bytes));
            channel.force(false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        size ++;
    }
}
