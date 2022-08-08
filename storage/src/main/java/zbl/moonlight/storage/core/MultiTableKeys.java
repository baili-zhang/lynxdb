package zbl.moonlight.storage.core;

import java.util.HashSet;
import java.util.List;

public class MultiTableKeys extends Pair<List<byte[]>, HashSet<Column>> {
    /**
     * @param left keys
     * @param right columns
     */
    public MultiTableKeys(List<byte[]> left, HashSet<Column> right) {
        super(left, right);
    }
}
