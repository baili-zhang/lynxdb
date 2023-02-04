package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.lsmtree.common.DbEntry;
import com.bailizhang.lynxdb.lsmtree.common.DbKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.lsmtree.config.Options;
import com.bailizhang.lynxdb.lsmtree.exception.DeletedException;
import com.bailizhang.lynxdb.lsmtree.schema.Column;
import com.bailizhang.lynxdb.lsmtree.schema.Key;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {
    private final Options options;
    private volatile boolean immutable = false;

    private final ConcurrentSkipListMap<Key, ConcurrentSkipListMap<Column, DbEntry>> skipListMap
            = new ConcurrentSkipListMap<>();

    private int size;
    private int walGlobalIndex;

    public MemTable(Options options) {
        this.options = options;
    }

    public void append(DbEntry dbEntry, int globalIndex) {
        if(immutable) {
            return;
        }

        if(options.wal()) {
            walGlobalIndex = globalIndex;
        }

        DbKey dbKey = dbEntry.key();
        Key key = new Key(dbKey.key());
        Column column = new Column(dbKey.column());

        ConcurrentSkipListMap<Column, DbEntry> columnMap = skipListMap.get(key);
        if(columnMap == null) {
            columnMap = new ConcurrentSkipListMap<>();
            skipListMap.put(key, columnMap);
        }

        if(columnMap.put(column, dbEntry) == null) {
            size ++;
        }
    }

    public byte[] find(DbKey dbKey) throws DeletedException {
        Key key = new Key(dbKey.key());
        Column column = new Column(dbKey.column());

        ConcurrentSkipListMap<Column, DbEntry> columnMap = skipListMap.get(key);
        if(columnMap == null) {
            return null;
        }

        DbEntry dbEntry = columnMap.get(column);
        if(dbEntry == null) {
            return null;
        }

        DbKey existed = dbEntry.key();

        if(existed.flag() == DbKey.DELETED) {
            throw new DeletedException();
        }

        return dbEntry.value();
    }

    public void find(byte[] key, HashSet<DbValue> values) {
        ConcurrentSkipListMap<Column, DbEntry> columnMap = skipListMap.get(new Key(key));
        if(columnMap == null) {
            return;
        }

        columnMap.forEach((column, dbEntry) -> {
            DbValue dbValue = new DbValue(column.bytes(), dbEntry.value());
            if(values.contains(dbValue)) {
                return;
            }
            values.add(dbValue);
        });
    }

    public boolean full() {
        return size >= options.memTableSize();
    }

    public void transformToImmutable() {
        immutable = true;
    }

    public List<DbEntry> all() {
        List<DbEntry> entries = new ArrayList<>();

        skipListMap.forEach(
                (key, columnMap) -> columnMap.forEach(
                        (column, dbEntry) -> entries.add(dbEntry)
                )
        );

        return entries;
    }

    public int maxWalGlobalIndex() {
        return walGlobalIndex;
    }
}
