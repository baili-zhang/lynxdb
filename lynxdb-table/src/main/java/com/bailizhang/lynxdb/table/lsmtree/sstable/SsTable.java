package com.bailizhang.lynxdb.table.lsmtree.sstable;

import com.bailizhang.lynxdb.core.common.FileType;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.entry.IndexEntry;
import com.bailizhang.lynxdb.table.entry.KeyEntry;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.level.Level;
import com.bailizhang.lynxdb.table.lsmtree.level.Levels;
import com.bailizhang.lynxdb.table.schema.Key;
import com.bailizhang.lynxdb.table.utils.BloomFilter;
import com.bailizhang.lynxdb.table.utils.Crc32cUtils;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.file.Path;
import java.util.*;

import static com.bailizhang.lynxdb.core.utils.PrimitiveTypeUtils.*;

public class SsTable {
    interface Default {
        int META_REGION_LENGTH_OFFSET = 0;
        int MAGIC_NUMBER_OFFSET = 4;
        int MEM_TABLE_SIZE_OFFSET = 8;
        int MAX_KEY_SIZE_OFFSET = 12;
        int KEY_SIZE_OFFSET = 16;
        int BLOOM_FILTER_REGION_LENGTH_OFFSET = 20;
        int FIRST_INDEX_REGION_LENGTH_OFFSET = 24;
        int SECOND_INDEX_REGION_LENGTH_OFFSET = 28;
        int DATA_REGION_LENGTH_OFFSET = 32;
        int CRC32C_OFFSET = 36;
    }

    private static final int META_HEADER_OFFSET = 0;
    private static final int META_HEADER_LENGTH = 44;

    private static final int SECOND_INDEX_ENTRY_LENGTH = BYTE_LENGTH + INT_LENGTH * 2 + LONG_LENGTH;

    private final MetaHeader metaHeader;

    private final MappedBuffer metaBuffer;
    private final BloomFilter bloomFilter;
    private final MappedBuffer firstIndexBuffer;
    private final MappedBuffer secondIndexBuffer;
    private final MappedBuffer dataBuffer;

    private final LogGroup valueLogGroup;

    public SsTable(
            Path baseDir,
            int ssTableNo,
            LogGroup logGroup
    ) {
        String filename = NameUtils.name(ssTableNo) + FileType.SSTABLE_FILE.suffix();
        Path filePath = FileUtils.createFileIfNotExisted(
                baseDir.toString(),
                filename
        ).toPath();

        MappedBuffer metaHeaderBuffer = new MappedBuffer(
                filePath,
                META_HEADER_OFFSET,
                META_HEADER_LENGTH
        );
        metaHeader = MetaHeader.from(metaHeaderBuffer.getBuffer());

        metaBuffer = new MappedBuffer(
                filePath,
                META_HEADER_OFFSET,
                metaHeader.metaRegionLength()
        );

        bloomFilter = BloomFilter.from(
                filePath,
                metaHeader.metaRegionLength(),
                metaHeader.maxKeySize()
        );

        int firstIndexRegionOffset = metaHeader.metaRegionLength() + metaHeader.bloomFilterRegionLength();
        firstIndexBuffer = new MappedBuffer(
                filePath,
                firstIndexRegionOffset,
                metaHeader.firstIndexRegionLength()
        );

        int secondIndexRegionOffset = firstIndexRegionOffset + metaHeader.firstIndexRegionLength();
        secondIndexBuffer = new MappedBuffer(
                filePath,
                secondIndexRegionOffset,
                metaHeader.secondIndexRegionLength()
        );

        int dataRegionOffset = secondIndexRegionOffset + metaHeader.secondIndexRegionLength();
        dataBuffer = new MappedBuffer(
                filePath,
                dataRegionOffset,
                metaHeader.dataRegionLength()
        );

        valueLogGroup = logGroup;
    }

    private SsTable(
            MetaHeader metaHeader,
            MappedBuffer metaBuffer,
            BloomFilter bloomFilter,
            MappedBuffer firstIndexBuffer,
            MappedBuffer secondIndexBuffer,
            MappedBuffer dataBuffer,
            LogGroup valueLogGroup
    ) {
        this.metaHeader = metaHeader;
        this.metaBuffer = metaBuffer;
        this.bloomFilter = bloomFilter;
        this.firstIndexBuffer = firstIndexBuffer;
        this.secondIndexBuffer = secondIndexBuffer;
        this.dataBuffer = dataBuffer;
        this.valueLogGroup = valueLogGroup;
    }

    public static SsTable create(
            Path baseDir,
            int ssTableNo,
            int levelNo,
            LsmTreeOptions options,
            List<KeyEntry> keyEntries,
            LogGroup valueLogGroup
    ) {
        String filename = NameUtils.name(ssTableNo) + FileType.SSTABLE_FILE.suffix();
        Path filePath = FileUtils.createFileIfNotExisted(
                baseDir.toString(),
                filename
        ).toPath();

        byte[] beginKey = keyEntries.getFirst().key();
        byte[] endKey = keyEntries.getLast().key();

        int metaRegionLength = META_HEADER_LENGTH + INT_LENGTH * 2 + LONG_LENGTH + beginKey.length + endKey.length;
        MappedBuffer metaBuffer = new MappedBuffer(filePath, META_HEADER_OFFSET, metaRegionLength);

        int maxKeySize = SsTable.maxKeySize(levelNo, options);
        if(keyEntries.size() > maxKeySize) {
            throw new RuntimeException();
        }

        BloomFilter bloomFilter = BloomFilter.from(
                filePath,
                metaRegionLength,
                maxKeySize
        );
        keyEntries.forEach(keyEntry -> bloomFilter.setObj(keyEntry.key()));

        int keySize = keyEntries.size();
        int memTableSize = options.memTableSize();

        int firstIndexRegionLength = 0;
        for(int i = 0; i < keySize; i += memTableSize) {
            KeyEntry keyEntry = keyEntries.get(i);
            byte[] key = keyEntry.key();
            firstIndexRegionLength += INT_LENGTH * 3 + LONG_LENGTH + key.length;
        }

        int secondIndexRegionLength = keySize * (BYTE_LENGTH + INT_LENGTH * 2 + LONG_LENGTH);

        int dataRegionLength = 0;
        for(int i = 0; i < keySize; i ++) {
            KeyEntry keyEntry = keyEntries.get(i);
            byte[] key = keyEntry.key();
            dataRegionLength += BYTE_LENGTH + INT_LENGTH * 2 + LONG_LENGTH * 2 + key.length;
        }

        // 初始化 MetaBuffer
        ByteBuffer buffer = metaBuffer.getBuffer();
        buffer.putInt(metaRegionLength);
        buffer.putInt(FileType.SSTABLE_FILE.magicNumber());
        buffer.putInt(memTableSize);
        buffer.putInt(maxKeySize);
        buffer.putInt(keySize);
        buffer.putInt(bloomFilter.length());
        buffer.putInt(firstIndexRegionLength);
        buffer.putInt(secondIndexRegionLength);
        buffer.putInt(dataRegionLength);
        long crc32c = Crc32cUtils.update(buffer, Default.CRC32C_OFFSET);
        buffer.putInt(beginKey.length);
        buffer.put(beginKey);
        buffer.putInt(endKey.length);
        buffer.put(endKey);
        Crc32cUtils.update(buffer);

        MetaHeader metaHeader = new MetaHeader(
                metaRegionLength,
                FileType.SSTABLE_FILE.magicNumber(),
                memTableSize,
                maxKeySize,
                keySize,
                bloomFilter.length(),
                firstIndexRegionLength,
                secondIndexRegionLength,
                dataRegionLength,
                crc32c
        );

        int firstIndexRegionOffset = metaRegionLength + bloomFilter.length();
        MappedBuffer firstIndexBuffer = new MappedBuffer(
                filePath,
                firstIndexRegionOffset,
                firstIndexRegionLength
        );

        int secondIndexRegionOffset = firstIndexRegionOffset + firstIndexRegionLength;
        MappedBuffer secondIndexBuffer = new MappedBuffer(
                filePath,
                secondIndexRegionOffset,
                secondIndexRegionLength
        );

        int dataRegionOffset = secondIndexRegionOffset + secondIndexRegionLength;
        MappedBuffer dataBuffer = new MappedBuffer(
                filePath,
                dataRegionOffset,
                dataRegionLength
        );

        return new SsTable(
                metaHeader,
                metaBuffer,
                bloomFilter,
                firstIndexBuffer,
                secondIndexBuffer,
                dataBuffer,
                valueLogGroup
        );
    }

    public void all(HashMap<Key, KeyEntry> entriesMap) {
        MappedByteBuffer indexMapperBuffer = secondIndexBuffer.getBuffer();
        indexMapperBuffer.rewind();

        MappedByteBuffer keyMappedBuffer = dataBuffer.getBuffer();
        keyMappedBuffer.rewind();

        // MemTable 可能不是最大容量
        while(BufferUtils.isNotOver(keyMappedBuffer)) {
            IndexEntry index = IndexEntry.from(indexMapperBuffer);
            int length = index.length();

            byte[] data = new byte[length];
            keyMappedBuffer.get(data);

            KeyEntry entry = KeyEntry.from(index.flag(), data);
            Key key = new Key(entry.key());

            KeyEntry oldEntry = entriesMap.get(key);
            if(oldEntry != null) {
                valueLogGroup.removeEntry(oldEntry.valueGlobalIndex());
            }
            entriesMap.put(key, entry);
        }
    }

    public boolean contains(byte[] key) {
        return bloomFilter.isExist(key);
    }

    public byte[] find(byte[] key) throws DeletedException, TimeoutException {
        int idx = findIdx(key);
        if(idx >= metaHeader.keySize()) {
            return null;
        }

        IndexEntry indexEntry = findIndexEntry(idx);

        if(indexEntry.flag() == Flags.DELETED) {
            throw new DeletedException();
        }

        KeyEntry keyEntry = findKeyEntry(indexEntry);
        if(keyEntry.isTimeout()) {
            throw new TimeoutException();
        }

        int globalIndex = keyEntry.valueGlobalIndex();
        LogEntry entry = valueLogGroup.findEntry(globalIndex);

        return entry == null ? null : entry.data();
    }

    public boolean existKey(byte[] key) throws DeletedException, TimeoutException {
        if(bloomFilter.isNotExist(key)) {
            return false;
        }

        int idx = findIdx(key);

        if(idx >= metaHeader.keySize()) {
            return false;
        }

        IndexEntry indexEntry = findIndexEntry(idx);
        KeyEntry keyEntry = findKeyEntry(indexEntry);

        if(Arrays.equals(key, keyEntry.key())) {
            if(keyEntry.isTimeout()) {
                throw new TimeoutException();
            }

            if(indexEntry.flag() == Flags.EXISTED) {
                return true;
            }

            throw new DeletedException();
        }

        return false;
    }

    public List<Key> rangeNext(
            byte[] beginKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        int idx = findIdxBiggerThan(beginKey);
        return range(
                idx,
                limit,
                deletedKeys,
                existedKeys,
                true
        );
    }

    public List<Key> rangeBefore(
            byte[] endKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        int idx = findIdxLessThan(endKey);
        return range(
                idx,
                limit,
                deletedKeys,
                existedKeys,
                false
        );
    }

    private List<Key> range(
            int idx,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys,
            boolean isRangeNext
    ) {
        List<Key> range = new ArrayList<>();

        while (limit > 0 && idx < metaHeader.keySize() && idx >= 0) {
            IndexEntry indexEntry = findIndexEntry(isRangeNext ? idx ++ : idx --);
            KeyEntry keyEntry = findKeyEntry(indexEntry);

            Key key = new Key(keyEntry.key());

            if(indexEntry.flag() == Flags.DELETED) {
                deletedKeys.add(key);
                continue;
            }

            if(deletedKeys.contains(key) || existedKeys.contains(key)) {
                continue;
            }

            existedKeys.add(key);
            range.add(key);

            limit --;
        }

        return range;
    }

    private static int maxKeySize(int levelNo, LsmTreeOptions options) {
        return options.memTableSize()
                * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, (levelNo - Levels.LEVEL_BEGIN));
    }

    // 找第一个大于等于 dbKey 的下标
    private int findIdx(byte[] key) {
        int begin = 0, end = metaHeader.keySize() - 1, mid, idx = metaHeader.keySize();

        while(begin <= end) {
            mid = begin + ((end - begin) >> 1);
            IndexEntry midIndexEntry = findIndexEntry(mid);
            KeyEntry midKeyEntry = findKeyEntry(midIndexEntry);

            if(Arrays.compare(key, midKeyEntry.key()) <= 0) {
                idx = mid;
                end = mid - 1;
            } else {
                begin = mid + 1;
            }
        }

        return idx;
    }

    private int findIdxBiggerThan(byte[] key) {
        Pair<KeyEntry, Integer> result = binarySearch(key);
        KeyEntry midKeyEntry = result.left();
        int idx = result.right();

        return Arrays.equals(key, midKeyEntry.key()) ? idx + 1 : idx;
    }

    private int findIdxLessThan(byte[] key) {
        Pair<KeyEntry, Integer> result = binarySearch(key);
        KeyEntry midKeyEntry = result.left();
        int idx = result.right();

        return Arrays.equals(key, midKeyEntry.key()) ? idx - 1 : idx;
    }

    private Pair<KeyEntry, Integer> binarySearch(byte[] key) {
        int begin = 0, end = metaHeader.keySize() - 1, mid, idx = metaHeader.keySize();

        IndexEntry midIndexEntry;
        KeyEntry midKeyEntry = null;

        while(begin <= end) {
            mid = begin + ((end - begin) >> 1);
            midIndexEntry = findIndexEntry(mid);
            midKeyEntry = findKeyEntry(midIndexEntry);

            if(Arrays.compare(key, midKeyEntry.key()) <= 0) {
                idx = mid;
                end = mid - 1;
            } else {
                begin = mid + 1;
            }
        }

        if(midKeyEntry == null) {
            throw new RuntimeException();
        }

        return new Pair<>(midKeyEntry, idx);
    }

    private IndexEntry findIndexEntry(int idx) {
        MappedByteBuffer indexMappedBuffer = secondIndexBuffer.getBuffer();
        indexMappedBuffer.position(idx * SECOND_INDEX_ENTRY_LENGTH);

        return IndexEntry.from(indexMappedBuffer);
    }

    private KeyEntry findKeyEntry(IndexEntry indexEntry) {
        ByteBuffer buffer = dataBuffer.getBuffer();
        buffer.position(indexEntry.begin());

        byte[] keyData = new byte[indexEntry.length()];
        buffer.get(keyData);

        return KeyEntry.from(indexEntry.flag(), keyData);
    }
}
