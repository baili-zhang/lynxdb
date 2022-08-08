package zbl.moonlight.storage.core;

import java.util.Arrays;

public record Column(byte[] val) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Column column = (Column) o;
        return Arrays.equals(val, column.val);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(val);
    }
}
