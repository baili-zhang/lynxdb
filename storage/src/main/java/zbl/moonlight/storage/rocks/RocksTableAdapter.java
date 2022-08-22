package zbl.moonlight.storage.rocks;

import org.rocksdb.RocksDBException;
import zbl.moonlight.storage.core.*;
import zbl.moonlight.storage.rocks.query.table.*;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;

public class RocksTableAdapter implements TableAdapter {

    private final RocksDatabase db;

    public RocksTableAdapter(String dataDir) {
        try {
            db = RocksDatabase.open(dataDir);
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SingleTableRow get(SingleTableKey key) {
        try {
            ResultSet<SingleTableRow> resultSet = new ResultSet<>();
            db.doQuery(new TableSingleGetQuery(key, resultSet));
            return resultSet.result();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public MultiTableRows get(MultiTableKeys keys) {
        try {
            ResultSet<MultiTableRows> resultSet = new ResultSet<>();
            db.doQuery(new TableBatchGetQuery(keys, resultSet));
            return resultSet.result();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(SingleTableRow row) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableSingleSetQuery(row, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void set(MultiTableRows rows) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableBatchInsertQuery(rows, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(byte[] key) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableSingleDeleteQuery(key, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void delete(List<byte[]> keys) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableBatchDeleteQuery(keys, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createColumn(byte[] column) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableSingleCreateColumnQuery(column, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void createColumns(Collection<byte[]> columns) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableBatchCreateColumnQuery(columns, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropColumn(byte[] column) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableSingleDeleteColumnQuery(column, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void dropColumns(HashSet<Column> columns) {
        try {
            ResultSet<Void> resultSet = new ResultSet<>();
            db.doQuery(new TableBatchDropColumnQuery(columns, resultSet));
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HashSet<Column> columns() {
        try {
            ResultSet<HashSet<Column>> resultSet = new ResultSet<>();
            db.doQuery(new TableBatchGetColumnQuery(resultSet));
            return resultSet.result();
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {
        db.close();
    }
}
