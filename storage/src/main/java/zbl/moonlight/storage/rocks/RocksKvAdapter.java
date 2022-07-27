package zbl.moonlight.storage.rocks;

import zbl.moonlight.storage.core.KvAdapter;
import zbl.moonlight.storage.core.Pair;

import java.util.List;

public class RocksKvAdapter implements KvAdapter {
    @Override
    public byte[] get(byte[] key) {
        return new byte[0];
    }

    @Override
    public List<byte[]> get(List<byte[]> keys) {
        return null;
    }

    @Override
    public void set(Pair<byte[], byte[]> kvPair) {

    }

    @Override
    public void set(List<Pair<byte[], byte[]>> kvPairs) {

    }

    @Override
    public void delete(byte[] key) {

    }

    @Override
    public void delete(List<byte[]> keys) {

    }
}
