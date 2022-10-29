package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.util.HashMap;

public class LevelTree {
    private final static int LEVEL_BEGIN = 1;

    private final HashMap<Integer, Level> levels = new HashMap<>();

    public LevelTree(String dir) {

    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null && level.isLessThan(key, column)) {
            byte[] value = level.find(key, column, timestamp);
            if(value != null) {
                return value;
            }

            level = levels.get(++ levelNo);
        }

        return null;
    }

    public void merge(MemTable immutable) {
        if(immutable == null) {
            return;
        }

        Level level = levels.get(LEVEL_BEGIN);
        if(level == null) {
            level = new Level();
            levels.put(LEVEL_BEGIN, level);
        }

        level.merge(immutable);

        if(level.isNotFull()) {
            return;
        }

        int levelNo = LEVEL_BEGIN;
        Level current = levels.get(levelNo);

        while (current.isFull()) {
            Level needMerge = current;
            current = levels.get(++ levelNo);

            if(current == null) {
                current = new Level();
                levels.put(levelNo, current);
            }

            current.merge(needMerge);
        }
    }

    public boolean delete(byte[] key, byte[] column, long timestamp) {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null && level.isLessThan(key, column)) {
            boolean hasDeleted = level.delete(key, column, timestamp);
            if(hasDeleted) {
                return hasDeleted;
            }

            level = levels.get(++ levelNo);
        }

        return false;
    }
}
