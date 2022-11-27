package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.log.LogOptions;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LevelTree {
    private final static int LEVEL_BEGIN = 1;
    private final static int EXTRA_DATA_LENGTH = 1;
    private final static String VALUE_GROUP_NAME = "value";

    private final HashMap<Integer, Level> levels = new HashMap<>();
    private final String baseDir;
    private final LogGroup valueFileGroup;
    private final Options options;

    public LevelTree(String dir, Options lsmOptions) {
        baseDir = dir;
        options = lsmOptions;

        // 初始化 valueFileGroup
        String valueFileGroupPath = Path.of(baseDir, VALUE_GROUP_NAME).toString();
        LogOptions logOptions = new LogOptions(EXTRA_DATA_LENGTH);

        valueFileGroup = new LogGroup(valueFileGroupPath, logOptions);

        List<String> subDirs = FileUtils.findSubDirs(baseDir);
        for(String subDir : subDirs) {
            int levelNo;

            try {
                levelNo = Integer.parseInt(subDir);
            } catch (NumberFormatException ignore) {
                continue;
            }

            String levelPath = Path.of(baseDir, subDir).toString();
            Level level = new Level(levelPath, levelNo, this, valueFileGroup, options);

            levels.put(levelNo, level);
        }
    }

    public byte[] find(DbKey dbKey) {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null) {
            if(level.contains(dbKey)) {
                byte[] value = level.find(dbKey);
                if(value != null) {
                    return value;
                }
            }

            level = levels.get(++ levelNo);
        }

        return null;
    }

    public List<DbValue> find(byte[] key) {
        List<DbValue> values = new ArrayList<>();

        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null) {
            values.addAll(level.find(key));
            level = levels.get(++ levelNo);
        }

        return values;
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

    public boolean delete(DbKey dbKey) {
        int levelNo = LEVEL_BEGIN;
        Level level = levels.get(levelNo);

        while(level != null && level.contains(dbKey)) {
            boolean hasDeleted = level.delete(dbKey);
            if(hasDeleted) {
                return hasDeleted;
            }

            level = levels.get(++ levelNo);
        }

        return false;
    }

    Level get(int levelNo) {
        return levels.get(levelNo);
    }

    void put(int levelNo, Level level) {
        levels.put(levelNo, level);
    }
}
