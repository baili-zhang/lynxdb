package zbl.moonlight.storage.query.cf;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.ColumnFamilyTuple;

import java.util.List;

public abstract class CfQuery implements CfQueryable {
    protected final List<ColumnFamilyTuple> tuples;

    protected List<ColumnFamilyHandle> columnFamilyHandles;
    protected List<ColumnFamilyDescriptor> columnFamilyDescriptors;

    protected CfQuery(List<ColumnFamilyTuple> tuples) {
        this.tuples = tuples;
    }

    public List<byte[]> columnFamilies() {
        return tuples.stream().map(ColumnFamilyTuple::columnFamily).map(ColumnFamily::value).toList();
    }

    public void setColumnFamilyHandle(List<ColumnFamilyHandle> handles) {
        columnFamilyHandles = handles;
    }

    public void setColumnFamilyDescriptors(List<ColumnFamilyDescriptor> descriptors) {
        columnFamilyDescriptors = descriptors;
    }
}
