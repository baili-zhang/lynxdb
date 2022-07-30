package zbl.moonlight.storage.rocks;

import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.*;

import java.util.List;

public class RocksTableAdapter implements TableAdapter {

    private final RocksDatabase db;

    public RocksTableAdapter(String name, String dataDir) {
        try {
            db = RocksDatabase.open(name, dataDir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SingleTableRow get(SingleTableKey key) {
        return null;
    }

    @Override
    public MultiTableRows get(MultiTableKeys keys) {
        return null;
    }

    @Override
    public void set(SingleTableKey key) {

    }

    @Override
    public void set(MultiTableKeys keys) {

    }

    @Override
    public void delete(byte[] key) {

    }

    @Override
    public void delete(List<byte[]> keys) {

    }

    @Override
    public void createColumn(byte[] column) {

    }

    @Override
    public void createColumns(List<byte[]> columns) {

    }

    @Override
    public void deleteColumn(byte[] column) {

    }

    @Override
    public void deleteColumns(List<byte[]> column) {

    }

    @Override
    public List<byte[]> columns() {
        return null;
    }
}
