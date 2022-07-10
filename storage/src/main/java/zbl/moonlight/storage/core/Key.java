package zbl.moonlight.storage.core;

import java.util.Arrays;

public record Key(byte[] value) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Key key = (Key) o;
        return Arrays.equals(value, key.value);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(value);
    }
}
