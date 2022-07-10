package zbl.moonlight.storage.query.cf;

import zbl.moonlight.storage.core.Queryable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public interface CfQueryable extends Queryable {
    byte[] DEFAULT_COLUMN_FAMILY = "default".getBytes(StandardCharsets.UTF_8);

    default boolean isDefault() {
        return Arrays.equals(DEFAULT_COLUMN_FAMILY, columnFamily());
    }

    byte[] columnFamily();
}
