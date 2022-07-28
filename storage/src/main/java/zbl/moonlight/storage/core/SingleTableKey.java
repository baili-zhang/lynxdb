package zbl.moonlight.storage.core;

import java.util.List;

public class SingleTableKey extends Pair<byte[], List<byte[]>>{
    public SingleTableKey(byte[] left, List<byte[]> right) {
        super(left, right);
    }
}
