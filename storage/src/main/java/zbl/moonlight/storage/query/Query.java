package zbl.moonlight.storage.query;

import org.rocksdb.ColumnFamilyHandle;

import java.util.List;

public abstract class Query implements Queryable {
    protected final List<QueryTuple> tuples;

    protected List<ColumnFamilyHandle> columnFamilyHandles;
    protected Query(List<QueryTuple> tuples) {
        this.tuples = tuples;
    }

    public void setColumnFamilyHandles(List<ColumnFamilyHandle> handles) {
        columnFamilyHandles = handles;
    }
}
