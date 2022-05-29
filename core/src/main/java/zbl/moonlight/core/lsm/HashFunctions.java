package zbl.moonlight.core.lsm;

/**
 * TODO: 应该用哪七个哈希函数做布隆过滤器的哈希函数
 */
public class HashFunctions {
    @HashFunction(0)
    public static int oneByOneHash(String key) {
        int hash, i;
        for (hash = 0, i = 0; i < key.length(); ++i) {
            hash += key.charAt(i);
            hash += (hash << 10);
            hash ^= (hash >> 6);
        }
        hash += (hash << 3);
        hash ^= (hash >> 11);
        hash += (hash << 15);
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(1)
    public static int bernstein(String key) {
        int hash = 0;
        int i;
        for (i = 0; i < key.length(); ++i)
            hash = 33 * hash + key.charAt(i);
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(2)
    public static int RSHash(String str) {
        int b = 378551;
        int a = 63689;
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = hash * a + str.charAt(i);
            a = a * b;
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(3)
    public static int JSHash(String str) {
        int hash = 1315423911;
        for (int i = 0; i < str.length(); i++) {
            hash ^= ((hash << 5) + str.charAt(i) + (hash >> 2));
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(4)
    public static int PJWHash(String str) {
        int BitsInUnsignedInt = 32;
        int ThreeQuarters = (BitsInUnsignedInt * 3) / 4;
        int OneEighth = BitsInUnsignedInt / 8;
        int HighBits = 0xFFFFFFFF << (BitsInUnsignedInt - OneEighth);
        int hash = 0;
        int test;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash << OneEighth) + str.charAt(i);

            if ((test = hash & HighBits) != 0) {
                hash = ((hash ^ (test >> ThreeQuarters)) & (~HighBits));
            }
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(5)
    public static int ELFHash(String str) {
        int hash = 0;
        int x;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash << 4) + str.charAt(i);
            if ((x = (int) (hash & 0xF0000000L)) != 0) {
                hash ^= (x >> 24);
                hash &= ~x;
            }
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(6)
    public static int BKDRHash(String str) {
        int seed = 131;
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = (hash * seed) + str.charAt(i);
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(7)
    public static int SDBMHash(String str) {
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash = str.charAt(i) + (hash << 6) + (hash << 16) - hash;
        }
        return (hash & 0x7FFFFFFF);
    }
    @HashFunction(8)
    public static int DJBHash(String str) {
        int hash = 5381;
        for (int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) + hash) + str.charAt(i);
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(9)
    public static int DEKHash(String str) {
        int hash = str.length();
        for (int i = 0; i < str.length(); i++) {
            hash = ((hash << 5) ^ (hash >> 27)) ^ str.charAt(i);
        }
        return (hash & 0x7FFFFFFF);
    }

    @HashFunction(10)
    public static int APHash(String str) {
        int hash = 0;
        for (int i = 0; i < str.length(); i++) {
            hash ^= ((i & 1) == 0) ? ((hash << 7) ^ str.charAt(i) ^ (hash >> 3))
                    : (~((hash << 11) ^ str.charAt(i) ^ (hash >> 5)));
        }
        return (hash & 0x7FFFFFFF);
    }
}
