package zbl.moonlight.core.lsm;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

class SSTableTest {
    private static final List<Entry<String, byte[]>> entries = new ArrayList<>();
    private SSTable SSTable;

    static {
        for (int i = 0; i < 1000; i++) {
            entries.add(Map.entry("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
    }

    @BeforeEach
    void beforeEach() throws IOException {
        SSTable = new SSTable(1, 1);
    }

    @AfterEach
    void afterEach() {
        SSTable.clear();
    }

    @Test
    void testMore() throws IOException {
        assert SSTable.isEmpty();

        for(Entry<String, byte[]> entry : entries) {
            SSTable.put(entry.getKey(), entry.getValue());
        }

        SSTable.close();
        SSTable = new SSTable(1, 1);

        assert SSTable.size() == 1000;
        assert SSTable.isFull();
    }

    @Test
    void containsKey() throws IOException {
        for(Entry<String, byte[]> entry : entries) {
            SSTable.put(entry.getKey(), entry.getValue());
        }

        SSTable.close();
        SSTable = new SSTable(1, 1);

        for(Entry<String, byte[]> entry : entries) {
            assert SSTable.containsKey(entry.getKey());
        }
    }

    @Test
    void get() throws IOException {
        for(Entry<String, byte[]> entry : entries) {
            SSTable.put(entry.getKey(), entry.getValue());
        }

        SSTable.close();
        SSTable = new SSTable(1, 1);

        for (int i = 0; i < 1000; i++) {
            byte[] value = SSTable.get("key" + i);
            assert ("value" + i).equals(new String(value));
        }
    }

    @Test
    void remove() throws IOException {
        for(int i = 0; i < 500; i ++) {
            var entry = entries.get(i);
            SSTable.put(entry.getKey(), entry.getValue());
        }

        SSTable.remove("key2");

        SSTable.close();
        SSTable = new SSTable(1, 1);

        assert SSTable.get("key2") == null;
    }

    @Test
    void putAll() throws IOException {
        HashMap<String, byte[]> map = new HashMap<>();
        for(Entry<String, byte[]> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }

        SSTable.putAll(map);

        SSTable.close();
        SSTable = new SSTable(1, 1);

        for (int i = 0; i < 1000; i++) {
            byte[] value = SSTable.get("key" + i);
            assert ("value" + i).equals(new String(value));
        }
    }

    @Test
    void clear() throws IOException {
        for(Entry<String, byte[]> entry : entries) {
            SSTable.put(entry.getKey(), entry.getValue());
        }

        SSTable.clear();

        SSTable.close();
        SSTable = new SSTable(1, 1);

        for (int i = 0; i < 1000; i++) {
            assert SSTable.get("key" + i) == null;
        }
    }

    @Test
    void keySet() {
    }

    @Test
    void values() {
    }

    @Test
    void entrySet() {
    }
}