package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.KcItem;
import com.bailizhang.lynxdb.lsmtree.memory.VersionalValue;
import com.bailizhang.lynxdb.lsmtree.utils.BloomFilter;

import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.Deque;

public class SsTable {
    private static final int BLOOM_FILTER_BEGIN_POSITION = 0;
    private static final int BLOOM_FILTER_BIT_COUNT = 10000;

    private final FileChannel channel;
    private final BloomFilter bloomFilter;

    private KcItem minKc;
    private KcItem maxKc;

    public SsTable(String dir, int id) {
        String filename = NameUtils.name(id);
        FileUtils.createFileIfNotExisted(dir, filename);
        channel = FileUtils.open(Path.of(dir, filename));
        bloomFilter = new BloomFilter(channel, BLOOM_FILTER_BEGIN_POSITION, BLOOM_FILTER_BIT_COUNT);
    }

    public void append(byte[] key, byte[] column, Deque<VersionalValue> values) {

    }

    public boolean isLessThan(byte[] key, byte[] column) {
        KcItem kcItem = new KcItem(key, column);
        return minKc.compareTo(kcItem) <= 0;
    }

    public boolean isBiggerThan(byte[] key, byte[] column) {
        return !isLessThan(key, column);
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        KcItem item = new KcItem(key, column);

        return null;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        return false;
    }
}
