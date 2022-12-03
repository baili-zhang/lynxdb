package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbIndex;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.common.Options;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class Level {
    public static final int LEVEL_SSTABLE_COUNT = 10;

    private final LogGroup valueFileGroup;
    private final String parentDir;
    private final Path baseDir;
    private final int levelNo;
    private final LevelTree parent;
    private final Options options;

    private List<SsTable> ssTables = new ArrayList<>(LEVEL_SSTABLE_COUNT);

    public Level(String dir, int level, LevelTree levelTree, LogGroup logGroup, Options lsmOptions) {
        parentDir = dir;
        baseDir = Path.of(dir, String.valueOf(level));
        FileUtils.createDirIfNotExisted(baseDir.toFile());

        levelNo = level;
        parent = levelTree;
        valueFileGroup = logGroup;
        options = lsmOptions;

        List<String> subs = FileUtils.findSubFiles(dir);
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

            ssTables.add(ssTable);
        }
    }

    public void merge(MemTable immutable) {
        if(isFull()) {
            mergeToNextLevel();
        }

        List<DbIndex> indexList = immutable.all()
                .stream()
                .map(entry -> {
                    int globalIndex = valueFileGroup.append(
                            ColumnFamilyRegion.EXISTED_ARRAY,
                            entry.value()
                    );
                    return new DbIndex(entry.key(), globalIndex);
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
        List<DbIndex> dbIndexList = new ArrayList<>();

        ssTables.forEach(ssTable -> dbIndexList.addAll(ssTable.dbIndexList()));
        dbIndexList.sort(Comparator.comparing(DbIndex::key));

        return dbIndexList;
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

    public List<DbValue> find(byte[] key) {
        List<DbValue> values = new ArrayList<>();

        for(SsTable ssTable : ssTables) {
            values.addAll(ssTable.find(key));
        }

        return values;
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

    private void mergeToNextLevel() {
        int nextLevelNo = levelNo + 1;
        Level nextLevel = parent.get(nextLevelNo);

        if(nextLevel == null) {
            nextLevel = new Level(parentDir, nextLevelNo, parent, valueFileGroup, options);
            parent.put(nextLevelNo, nextLevel);
        }

        nextLevel.merge(this);

        FileUtils.deleteSubs(baseDir);
        ssTables = new ArrayList<>(LEVEL_SSTABLE_COUNT);
    }

    private void createNextSsTable(List<DbIndex> indexList) {
        int nextSsTableNo = ssTables.size();
        Path nextSsTablePath = Path.of(baseDir.toString(), NameUtils.name(nextSsTableNo));
        SsTable ssTable = SsTable.create(
                nextSsTablePath,
                levelNo,
                options,
                indexList,
                valueFileGroup
        );
        ssTables.add(ssTable);
    }
}
