package zbl.moonlight.storage.core;

import java.util.List;

public class MultiTableKeys extends Pair<List<byte[]>, List<byte[]>> {
    public MultiTableKeys(List<byte[]> left, List<byte[]> right) {
        super(left, right);
    }
}
