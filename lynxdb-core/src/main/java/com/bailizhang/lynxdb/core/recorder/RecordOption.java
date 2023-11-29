package com.bailizhang.lynxdb.core.recorder;

import java.util.Objects;

public record RecordOption(
        String name,
        RecordUnit unit
) {
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RecordOption that = (RecordOption) o;
        return Objects.equals(name, that.name) && unit == that.unit;
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, unit);
    }
}
