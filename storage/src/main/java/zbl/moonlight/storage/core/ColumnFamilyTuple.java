package zbl.moonlight.storage.core;

import java.util.Objects;

public record ColumnFamilyTuple(ColumnFamily columnFamily, Key key, Value value) {
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
        ColumnFamilyTuple tuple = (ColumnFamilyTuple) o;
        return Objects.equals(columnFamily, tuple.columnFamily) && Objects.equals(key, tuple.key) && Objects.equals(value, tuple.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(columnFamily, key, value);
    }
}
