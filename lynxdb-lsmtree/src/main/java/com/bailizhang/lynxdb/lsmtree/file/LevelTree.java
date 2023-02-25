package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogGroupOptions;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.config.LsmTreeOptions;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;

public class LevelTree {
    public final static int LEVEL_BEGIN = 1;

    private final static int EXTRA_DATA_LENGTH = 1;
    private final static String VALUE_GROUP_NAME = "value";

    private final HashMap<Integer, Level> levels = new HashMap<>();
    private final String baseDir;
    private final LogGroup valueFileGroup;
    private final LsmTreeOptions options;

    public LevelTree(String dir, LsmTreeOptions lsmOptions) {
        baseDir = dir;
        options = lsmOptions;

        // 初始化 valueFileGroup
        String valueFileGroupPath = Path.of(baseDir, VALUE_GROUP_NAME).toString();
        LogGroupOptions logOptions = new LogGroupOptions(EXTRA_DATA_LENGTH);

        valueFileGroup = new LogGroup(valueFileGroupPath, logOptions);

        List<String> subDirs = FileUtils.findSubDirs(baseDir);
        for(String subDir : subDirs) {
            int levelNo;

            try {
                levelNo = Integer.parseInt(subDir);
            } catch (NumberFormatException ignore) {
                continue;
            }

            Level level = new Level(baseDir, levelNo, this, valueFileGroup, options);

            levels.put(levelNo, level);
        }
    }

    public byte[] find(byte[] key) throws DeletedException {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null) {
            if(level.contains(key)) {
                byte[] value = level.find(key);
                if(value != null) {
                    return value;
                }
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
            level = new Level(baseDir, LEVEL_BEGIN, this, valueFileGroup, options);
            levels.put(LEVEL_BEGIN, level);
        }

        level.merge(immutable);
    }

    Level get(int levelNo) {
        return levels.get(levelNo);
    }

    void put(int levelNo, Level level) {
        levels.put(levelNo, level);
    }

    public boolean existKey(byte[] key) {
        throw new UnsupportedOperationException();
    }

    public List<Key> range(byte[] beginKey, int limit) {
        // TODO
        throw new UnsupportedOperationException();
    }
}
