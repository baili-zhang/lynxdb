package zbl.moonlight.storage.core;

import java.util.Set;

public class SingleTableKey extends Pair<byte[], Set<Column>>{
    /**
     * @param left key
     * @param right columns
     */
    public SingleTableKey(byte[] left, Set<Column> right) {
        super(left, right);
    }
}
