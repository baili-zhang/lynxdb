package com.bailizhang.lynxdb.lsmtree.file;

import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.lsmtree.log.WriteAheadLog;
import com.bailizhang.lynxdb.lsmtree.memory.MemTable;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class ColumnFamilyRegion {
    private final WriteAheadLog wal;
    private MemTable immutable;
    private MemTable mutable;
    private List<Level> levels = new ArrayList<>();

    public ColumnFamilyRegion(String dir,  byte[] columnFamily) {
        Path path = Path.of(dir, G.I.toString(columnFamily));
        wal = new WriteAheadLog(path.toString());
        mutable = new MemTable();
    }

    public byte[] find(byte[] key, byte[] column, long timestamp) {
        return null;
    }

    public void insert(byte[] key, byte[] column, long timestamp, byte[] value) {
        wal.append(key, column, timestamp);
        mutable.append(key, column, timestamp);
    }

    public void delete(byte[] key, byte[] column, long timestamp) {
    }

    public void addColumn(byte[] column) {
    }

    public void removeColumn(byte[] column) {
    }
}
