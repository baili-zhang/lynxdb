package zbl.moonlight.storage.core;

import java.util.HashSet;
import java.util.List;

public interface TableAdapter {
    SingleTableRow get(SingleTableKey key);
    MultiTableRows get(MultiTableKeys keys);
    void set(SingleTableRow key);
    void set(MultiTableRows keys);
    void delete(byte[] key);
    void delete(List<byte[]> keys);
    void createColumn(byte[] column);
    void createColumns(List<byte[]> columns);
    void deleteColumn(byte[] column);
    void deleteColumns(HashSet<Column> column);
    List<byte[]> columns();
}
