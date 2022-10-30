package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.memory.SkipListNode;

import java.util.ArrayList;
import java.util.List;

public class Level {
    private static final int LEVEL_SSTABLE_COUNT = 10;

    private final List<SsTable> ssTables = new ArrayList<>(LEVEL_SSTABLE_COUNT);
    private final String baseDir;

    public Level(String dir) {
        baseDir = dir;

        List<String> subs = FileUtils.findSubFiles(dir);
        for(String sub : subs) {
            int id = Integer.parseInt(sub);
            ssTables.add(new SsTable(baseDir, id));
        }
    }

    public void merge(MemTable immutable) {
        SsTable ssTable = new SsTable(baseDir, ssTables.size());
        for(SkipListNode node : immutable) {
            ssTable.append(node.key(), node.column(), node.values());
        }
        ssTables.add(ssTable);
    }

    public void merge(Level level) {

    }

    public boolean isNotFull() {
        return !isFull();
    }

    public boolean isFull() {
        return ssTables.size() >= LEVEL_SSTABLE_COUNT;
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.isBiggerThan(key, column)) {
                continue;
            }

            byte[] value = ssTable.find(key, column, timestamp);
            if(value != null) {
                return value;
            }
        }
        return null;
    }

    public boolean isLessThan(byte[] key, byte[] column) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.isLessThan(key, column)) {
                return true;
            }
        }

        return false;
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.isBiggerThan(key, column)) {
                continue;
            }

            if(ssTable.delete(key, column, timestamp)) {
                return true;
            }
        }
        return false;
    }
}
