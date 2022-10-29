package com.bailizhang.lynxdb.lsmtree.memory;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.*;

class SkipListTest {


    @Test
    void test_001() {
        SkipList skipList = new SkipList();
        long v1 = 10L;
        long v2 = 11L;

        skipList.insert(
                "key1".getBytes(),
                "column1".getBytes(),
                v1,
                "value1v1".getBytes()
        );

        skipList.insert(
                "key1".getBytes(),
                "column1".getBytes(),
                v2,
                "value1v2".getBytes()
        );

        skipList.insert(
                "key2".getBytes(),
                "column2".getBytes(),
                v1,
                "value2v1".getBytes()
        );

        skipList.insert(
                "key2".getBytes(),
                "column2".getBytes(),
                v2,
                "value2v2".getBytes()
        );

        byte[] value1v1 = skipList.find(
                "key1".getBytes(),
                "column1".getBytes(),
                v1);

        assert Arrays.equals(value1v1, "value1v1".getBytes());

        byte[] value1v2 = skipList.find(
                "key1".getBytes(),
                "column1".getBytes(),
                v2);

        assert Arrays.equals(value1v2, "value1v2".getBytes());

        byte[] value2v1 = skipList.find(
                "key2".getBytes(),
                "column2".getBytes(),
                v1);

        assert Arrays.equals(value2v1, "value2v1".getBytes());

        byte[] value2v2 = skipList.find(
                "key2".getBytes(),
                "column2".getBytes(),
                v2);

        assert Arrays.equals(value2v2, "value2v2".getBytes());

        skipList.delete(
                "key2".getBytes(),
                "column2".getBytes(),
                v2);

        byte[] newValue2v2 = skipList.find(
                "key2".getBytes(),
                "column2".getBytes(),
                v2);

        assert newValue2v2 == null;
    }
}