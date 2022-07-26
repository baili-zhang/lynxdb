package zbl.moonlight.storage.query;

import zbl.moonlight.storage.core.ColumnFamily;
import zbl.moonlight.storage.core.Key;
import zbl.moonlight.storage.core.Value;

import java.util.Objects;

public record QueryTuple(Key key, ColumnFamily columnFamily, Value value) {
    public byte[] keyBytes() {
        return key.value();
    }

    public byte[] valueBytes() {
        return value.value();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        QueryTuple tuple = (QueryTuple) o;
        return Objects.equals(columnFamily, tuple.columnFamily) && Objects.equals(key, tuple.key) && Objects.equals(value, tuple.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnFamily, key, value);
    }
}
