package com.bailizhang.lynxdb.lsmtree.memory;

import com.bailizhang.lynxdb.core.common.WrappedBytes;
import com.bailizhang.lynxdb.lsmtree.common.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemTable {
    private final Options options;
    private volatile boolean immutable = false;

    private final ConcurrentSkipListMap<Key, ConcurrentSkipListMap<Column, byte[]>> skipListMap
            = new ConcurrentSkipListMap<>();

    private int size;

    public MemTable(Options options) {
        this.options = options;
    }

    public void append(DbEntry dbEntry) {
        if(immutable) {
            return;
        }

        DbKey dbKey = dbEntry.key();
        Key key = new Key(dbKey.key());
        Column column = new Column(dbKey.column());

        ConcurrentSkipListMap<Column, byte[]> columnMap = skipListMap.get(key);
        if(columnMap == null) {
            columnMap = new ConcurrentSkipListMap<>();
            skipListMap.put(key, columnMap);
        }

        if(columnMap.put(column, dbEntry.value()) == null) {
            size ++;
        }
    }

    public byte[] find(DbKey dbKey) {
        Key key = new Key(dbKey.key());
        Column column = new Column(dbKey.column());

        ConcurrentSkipListMap<Column, byte[]> columnMap = skipListMap.get(key);
        if(columnMap == null) {
            return null;
        }

        return columnMap.get(column);
    }

    public List<DbValue> find(byte[] key) {
        List<DbValue> values = new ArrayList<>();

        ConcurrentSkipListMap<Column, byte[]> columnMap = skipListMap.get(new Key(key));
        if(columnMap == null) {
            return values;
        }

        columnMap.forEach((column, value) -> values.add(new DbValue(column.bytes(), value)));

        return values;
    }

    public boolean full() {
        return size >= options.memTableSize();
    }

    public void transformToImmutable() {
        immutable = true;
    }

    public boolean delete(DbKey dbKey) {
        Key key = new Key(dbKey.key());
        Column column = new Column(dbKey.column());

        ConcurrentSkipListMap<Column, byte[]> columnMap = skipListMap.get(key);
        if(columnMap == null) {
            return false;
        }

        return columnMap.remove(column) != null;
    }
}
