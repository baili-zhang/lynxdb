package zbl.moonlight.storage.core;

import java.util.List;

public interface TableAdapter {
    SingleTableRow get(SingleTableKey key);
    MultiTableRows get(MultiTableKeys keys);
    void set(SingleTableKey key);
    void set(MultiTableKeys keys);
    void delete(byte[] key);
    void delete(List<byte[]> keys);
    void createColumn(byte[] column);
    void createColumns(List<byte[]> columns);
    void deleteColumn(byte[] column);
    void deleteColumns(List<byte[]> column);
    List<byte[]> columns();
}
