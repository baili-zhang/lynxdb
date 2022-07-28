package zbl.moonlight.storage.core;

import java.util.List;

public class SingleTableRow extends Pair<byte[], List<byte[]>> {
    public SingleTableRow(byte[] left, List<byte[]> right) {
        super(left, right);
    }
}
