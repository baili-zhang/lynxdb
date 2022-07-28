package zbl.moonlight.storage.core;

import java.util.List;

public class MultiTableRows extends Pair<List<byte[]>, List<byte[]>>{
    public MultiTableRows(List<byte[]> left, List<byte[]> right) {
        super(left, right);
    }
}
