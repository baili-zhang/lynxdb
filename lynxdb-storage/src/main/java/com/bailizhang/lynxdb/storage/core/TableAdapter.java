package com.bailizhang.lynxdb.storage.core;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public interface TableAdapter extends AutoCloseable {
    SingleTableRow get(SingleTableKey key);
    MultiTableRows get(MultiTableKeys keys);
    void set(SingleTableRow row);
    void set(MultiTableRows rows);
    void delete(byte[] key);
    void delete(List<byte[]> keys);
    void createColumn(byte[] column);
    void createColumns(Collection<byte[]> columns);
    void dropColumn(byte[] column);
    void dropColumns(HashSet<Column> column);
    HashSet<Column> columns();
}
