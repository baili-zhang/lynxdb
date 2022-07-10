package zbl.moonlight.storage.core;

import java.util.Arrays;

public record ColumnFamily (byte[] value) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnFamily that = (ColumnFamily) o;
        return Arrays.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
