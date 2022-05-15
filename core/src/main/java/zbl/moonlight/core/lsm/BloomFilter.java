package zbl.moonlight.core.lsm;

public class BloomFilter {
    private final byte[] bitmap;

    BloomFilter(byte[] bitmap) {
        this.bitmap = bitmap;
    }

    public void set(int index) {
        int i = index >> 3;
        byte b = (byte) (1 << (index & 0x07));
        bitmap[i] &= b;
    }

    public void unSet(int index) {
        int i = index >> 3;
        byte b = (byte) ~ (1 << (index & 0x07));
        bitmap[i] &= b;
    }

    public boolean isSet(int index) {
        return false;
    }

    public boolean isExist(String key) {
        return false;
    }
}
