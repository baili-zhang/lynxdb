package com.bailizhang.lynxdb.core.utils;

import org.junit.jupiter.api.Test;

class ByteArrayUtilsTest {

    @Test
    void compare() {
        byte[] origin = new byte[]{1, 2};
        byte[] equals = new byte[]{1, 2};
        byte[] b1 = new byte[]{2, 2};
        byte[] l1 = new byte[]{1, 1};
        byte[] b2 = new byte[]{1, 2, 1};
        byte[] l2 = new byte[]{1};

        assert ByteArrayUtils.compare(origin, equals) == 0;
        assert ByteArrayUtils.compare(origin, b1) < 0;
        assert ByteArrayUtils.compare(origin, l1) > 0;
        assert ByteArrayUtils.compare(origin, b2) < 0;
        assert ByteArrayUtils.compare(origin, l2) > 0;
    }
}