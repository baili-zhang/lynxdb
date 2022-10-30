package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.KcItem;
import com.bailizhang.lynxdb.lsmtree.memory.VersionalValue;

import java.io.File;
import java.util.Deque;

public class SsTable {
    private KcItem minKc;
    private KcItem maxKc;

    public SsTable(String dir, int id) {
        String filename = NameUtils.name(id);
        File file = FileUtils.createFileIfNotExisted(dir, filename);
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
        return null;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        return false;
    }
}
