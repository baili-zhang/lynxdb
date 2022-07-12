package zbl.moonlight.storage.query;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import zbl.moonlight.storage.core.ColumnFamily;

import java.util.List;

public abstract class Query implements Queryable {
    protected final List<QueryTuple> tuples;

    protected List<ColumnFamilyHandle> columnFamilyHandles;
    protected List<ColumnFamilyDescriptor> columnFamilyDescriptors;

    protected Query(List<QueryTuple> tuples) {
        this.tuples = tuples;
    }

    public List<byte[]> columnFamilies() {
        return tuples.stream().map(QueryTuple::columnFamily).map(ColumnFamily::value).toList();
    }

    public void setColumnFamilyHandle(List<ColumnFamilyHandle> handles) {
        columnFamilyHandles = handles;
    }

    public void setColumnFamilyDescriptors(List<ColumnFamilyDescriptor> descriptors) {
        columnFamilyDescriptors = descriptors;
    }
}
