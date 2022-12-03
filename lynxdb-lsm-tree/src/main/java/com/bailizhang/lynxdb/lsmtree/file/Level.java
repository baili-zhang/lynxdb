package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbIndex;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.nio.file.Path;
import java.util.*;

import static com.bailizhang.lynxdb.lsmtree.common.DbKey.EXISTED;
import static com.bailizhang.lynxdb.lsmtree.common.DbKey.EXISTED_ARRAY;

public class Level {
    public static final int LEVEL_SSTABLE_COUNT = 10;

    private final LogGroup valueFileGroup;
    private final String parentDir;
    private final Path baseDir;
    private final int levelNo;
    private final LevelTree parent;
    private final Options options;

    private LinkedList<SsTable> ssTables = new LinkedList<>();

    public Level(String dir, int level, LevelTree levelTree, LogGroup logGroup, Options lsmOptions) {
        parentDir = dir;
        baseDir = Path.of(dir, String.valueOf(level));
        FileUtils.createDirIfNotExisted(baseDir.toFile());

        levelNo = level;
        parent = levelTree;
        valueFileGroup = logGroup;
        options = lsmOptions;

        List<String> subs = FileUtils.findSubFiles(baseDir);
        subs.sort(Comparator.comparingInt(NameUtils::id));

        for(String sub : subs) {
            int id = NameUtils.id(sub);

            Path filePath = Path.of(
                    baseDir.toString(),
                    NameUtils.name(id)
            );

            SsTable ssTable = new SsTable(
                    filePath,
                    levelNo,
                    valueFileGroup,
                    options
            );

            if(ssTables.size() > LEVEL_SSTABLE_COUNT) {
                throw new RuntimeException();
            }

            ssTables.addFirst(ssTable);
        }
    }

    public void merge(MemTable immutable) {
        if(isFull()) {
            mergeToNextLevel();
        }

        List<DbIndex> indexList = immutable.all()
                .stream()
                .map(entry -> {
                    DbKey dbKey = entry.key();
                    int globalIndex = -1;

                    if(dbKey.flag() == EXISTED) {
                        globalIndex = valueFileGroup.append(
                                EXISTED_ARRAY,
                                entry.value()
                        );
                    }

                    return new DbIndex(dbKey, globalIndex);
                }).toList();

        createNextSsTable(indexList);
    }

    public void merge(Level level) {
        if(isFull()) {
            mergeToNextLevel();
        }

        createNextSsTable(level.all());
    }

    public List<DbIndex> all() {
        HashSet<DbIndex> dbIndexSet = new HashSet<>();

        ssTables.forEach(ssTable -> ssTable.all(dbIndexSet));
        List<DbIndex> dbIndexList = new ArrayList<>(dbIndexSet);
        dbIndexList.sort(Comparator.comparing(DbIndex::key));

        return dbIndexList;
    }

    public boolean isFull() {
        return ssTables.size() >= LEVEL_SSTABLE_COUNT;
    }

    public byte[] find(DbKey dbKey) throws DeletedException {
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

    public void find(byte[] key, HashSet<DbValue> dbValues) {
        for(SsTable ssTable : ssTables) {
            ssTable.find(key, dbValues);
        }
    }

    public boolean contains(DbKey dbKey) {
        for(SsTable ssTable : ssTables) {
            if(ssTable.contains(dbKey)) {
                return true;
            }
        }

        return false;
    }

    private void mergeToNextLevel() {
        int nextLevelNo = levelNo + 1;
        Level nextLevel = parent.get(nextLevelNo);

        if(nextLevel == null) {
            nextLevel = new Level(parentDir, nextLevelNo, parent, valueFileGroup, options);
            parent.put(nextLevelNo, nextLevel);
        }

        nextLevel.merge(this);

        FileUtils.deleteSubs(baseDir);
        ssTables = new LinkedList<>();
    }

    private void createNextSsTable(List<DbIndex> dbIndexList) {
        int nextSsTableNo = ssTables.size();
        Path nextSsTablePath = Path.of(baseDir.toString(), NameUtils.name(nextSsTableNo));
        SsTable ssTable = SsTable.create(
                nextSsTablePath,
                levelNo,
                options,
                dbIndexList,
                valueFileGroup
        );
        ssTables.addFirst(ssTable);
    }
}
