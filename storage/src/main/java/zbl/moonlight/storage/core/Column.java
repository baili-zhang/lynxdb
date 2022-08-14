package zbl.moonlight.storage.core;

import java.util.Arrays;

public record Column(byte[] value) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Arrays.equals(value, column.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
