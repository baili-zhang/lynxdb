package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;
import com.bailizhang.lynxdb.lsmtree.memory.SkipListNode;

import java.util.ArrayList;
import java.util.List;

public class Level {
    public static final int LEVEL_SSTABLE_COUNT = 10;

    private final LogGroup valueFileGroup;
    private final List<SsTable> ssTables = new ArrayList<>(LEVEL_SSTABLE_COUNT);
    private final String baseDir;
    private final int levelNo;
    private final Options options;

    public Level(String dir, int level, LogGroup logGroup, Options lsmOptions) {
        baseDir = dir;

        levelNo = level;
        valueFileGroup = logGroup;
        options = lsmOptions;

        List<String> subs = FileUtils.findSubFiles(dir);
        for(String sub : subs) {
            int id = Integer.parseInt(sub);

            SsTable ssTable = new SsTable(
                    baseDir,
                    id,
                    levelNo,
                    valueFileGroup,
                    options
            );

            ssTables.add(ssTable);
        }
    }

    public void merge(MemTable immutable) {
        SsTable ssTable = new SsTable(
                baseDir,
                ssTables.size(),
                levelNo,
                valueFileGroup,
                options
        );

        for(SkipListNode node : immutable) {
            ssTable.append(node.key(), node.column(), node.value());
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

    public byte[] find(DbKey dbKey) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.contains(dbKey)) {
                byte[] value = ssTable.find(dbKey);
                if(value != null) {
                    return value;
                }
            }
        }
        return null;
    }

    public boolean contains(DbKey dbKey) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.contains(dbKey)) {
                return true;
            }
        }

        return false;
    }

    public boolean delete(DbKey dbKey) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.contains(dbKey) && ssTable.delete(dbKey)) {
                return true;
            }
        }
        return false;
    }
}
