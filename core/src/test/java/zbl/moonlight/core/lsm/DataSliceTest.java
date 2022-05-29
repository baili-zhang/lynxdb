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

class DataSliceTest {
    private static final List<Entry<String, byte[]>> entries = new ArrayList<>();
    private DataSlice dataSlice;

    static {
        for (int i = 0; i < 1000; i++) {
            entries.add(Map.entry("key" + i, ("value" + i).getBytes(StandardCharsets.UTF_8)));
        }
    }

    @BeforeEach
    void beforeEach() throws IOException {
        dataSlice = new DataSlice(1, 1);
    }

    @AfterEach
    void afterEach() {
        dataSlice.clear();
    }

    @Test
    void testMore() throws IOException {
        assert dataSlice.isEmpty();

        for(Entry<String, byte[]> entry : entries) {
            dataSlice.put(entry.getKey(), entry.getValue());
        }

        dataSlice.close();
        dataSlice = new DataSlice(1, 1);

        assert dataSlice.size() == 1000;
        assert dataSlice.isFull();
    }

    @Test
    void containsKey() throws IOException {
        for(Entry<String, byte[]> entry : entries) {
            dataSlice.put(entry.getKey(), entry.getValue());
        }

        dataSlice.close();
        dataSlice = new DataSlice(1, 1);

        for(Entry<String, byte[]> entry : entries) {
            assert dataSlice.containsKey(entry.getKey());
        }
    }

    @Test
    void get() throws IOException {
        for(Entry<String, byte[]> entry : entries) {
            dataSlice.put(entry.getKey(), entry.getValue());
        }

        dataSlice.close();
        dataSlice = new DataSlice(1, 1);

        for (int i = 0; i < 1000; i++) {
            byte[] value = dataSlice.get("key" + i);
            assert ("value" + i).equals(new String(value));
        }
    }

    @Test
    void remove() throws IOException {
        for(int i = 0; i < 500; i ++) {
            var entry = entries.get(i);
            dataSlice.put(entry.getKey(), entry.getValue());
        }

        dataSlice.remove("key2");

        dataSlice.close();
        dataSlice = new DataSlice(1, 1);

        assert dataSlice.get("key2") == null;
    }

    @Test
    void putAll() throws IOException {
        HashMap<String, byte[]> map = new HashMap<>();
        for(Entry<String, byte[]> entry : entries) {
            map.put(entry.getKey(), entry.getValue());
        }

        dataSlice.putAll(map);

        dataSlice.close();
        dataSlice = new DataSlice(1, 1);

        for (int i = 0; i < 1000; i++) {
            byte[] value = dataSlice.get("key" + i);
            assert ("value" + i).equals(new String(value));
        }
    }

    @Test
    void clear() throws IOException {
        for(Entry<String, byte[]> entry : entries) {
            dataSlice.put(entry.getKey(), entry.getValue());
        }

        dataSlice.clear();

        dataSlice.close();
        dataSlice = new DataSlice(1, 1);

        for (int i = 0; i < 1000; i++) {
            assert dataSlice.get("key" + i) == null;
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