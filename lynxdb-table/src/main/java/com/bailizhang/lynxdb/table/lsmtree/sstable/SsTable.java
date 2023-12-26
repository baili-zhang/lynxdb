/*
 * Copyright 2022-2023 Baili Zhang.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bailizhang.lynxdb.table.lsmtree.sstable;

import com.bailizhang.lynxdb.core.common.FileType;
import com.bailizhang.lynxdb.core.common.Flags;
import com.bailizhang.lynxdb.core.common.Pair;
import com.bailizhang.lynxdb.core.log.LogEntry;
import com.bailizhang.lynxdb.core.log.LogGroup;
import com.bailizhang.lynxdb.core.mmap.MappedBuffer;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.core.utils.Crc32cUtils;
import com.bailizhang.lynxdb.core.utils.FileUtils;
import com.bailizhang.lynxdb.core.utils.NameUtils;
import com.bailizhang.lynxdb.table.config.LsmTreeOptions;
import com.bailizhang.lynxdb.table.exception.DeletedException;
import com.bailizhang.lynxdb.table.exception.TimeoutException;
import com.bailizhang.lynxdb.table.lsmtree.level.Level;
import com.bailizhang.lynxdb.table.lsmtree.level.Levels;
import com.bailizhang.lynxdb.table.schema.Key;
import com.bailizhang.lynxdb.table.utils.BloomFilter;

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
    public static final int META_HEADER_LENGTH = 44;

    private static final int SECOND_INDEX_ENTRY_LENGTH = BYTE_LENGTH + INT_LENGTH * 2 + LONG_LENGTH;

    private final MetaHeader metaHeader;
    private final byte[] beginKey;
    private final byte[] endKey;
    private final List<FirstIndexEntry> firstIndexEntries;

    private final BloomFilter bloomFilter;
    private final MappedBuffer firstIndexBuffer;
    private final MappedBuffer secondIndexBuffer;
    private final MappedBuffer dataBuffer;

    private final LogGroup valueLogGroup;

    /**
     * Load SSTable from exist file.
     *
     * @param baseDir Base directory
     * @param ssTableNo SSTable No.
     * @param logGroup Value log group
     */
    public SsTable(
            Path baseDir,
            int ssTableNo,
            LogGroup logGroup
    ) {
        String filename = NameUtils.name(ssTableNo) + FileType.SSTABLE_FILE.suffix();
        Path filePath = Path.of(baseDir.toString(), filename);

        if(FileUtils.notExist(filePath)) {
            throw new RuntimeException();
        }

        MappedBuffer metaHeaderBuffer = new MappedBuffer(
                filePath,
                META_HEADER_OFFSET,
                META_HEADER_LENGTH
        );
        metaHeader = MetaHeader.from(metaHeaderBuffer.getBuffer());

        MappedBuffer metaKeyBuffer = new MappedBuffer(
                filePath,
                META_HEADER_LENGTH,
                metaHeader.metaRegionLength() - META_HEADER_LENGTH
        );
        MappedByteBuffer buffer = metaKeyBuffer.getBuffer();
        Crc32cUtils.check(buffer);
        beginKey = BufferUtils.getBytes(buffer);
        endKey = BufferUtils.getBytes(buffer);

        bloomFilter = BloomFilter.from(
                filePath,
                metaHeader.metaRegionLength(),
                metaHeader.maxKeyAmount()
        );

        int firstIndexRegionOffset = metaHeader.metaRegionLength() + metaHeader.bloomFilterRegionLength();
        firstIndexBuffer = new MappedBuffer(
                filePath,
                firstIndexRegionOffset,
                metaHeader.firstIndexRegionLength()
        );
        firstIndexEntries = new ArrayList<>();
        ByteBuffer firstIndexRawBuffer = firstIndexBuffer.getBuffer();
        while (BufferUtils.isNotOver(firstIndexRawBuffer)) {
            firstIndexEntries.add(FirstIndexEntry.from(firstIndexRawBuffer));
        }

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
            byte[] beginKey,
            byte[] endKey,
            List<FirstIndexEntry> firstIndexEntries,
            BloomFilter bloomFilter,
            MappedBuffer firstIndexBuffer,
            MappedBuffer secondIndexBuffer,
            MappedBuffer dataBuffer,
            LogGroup valueLogGroup
    ) {
        this.metaHeader = metaHeader;
        this.beginKey = beginKey;
        this.endKey = endKey;
        this.firstIndexEntries = firstIndexEntries;
        this.bloomFilter = bloomFilter;
        this.firstIndexBuffer = firstIndexBuffer;
        this.secondIndexBuffer = secondIndexBuffer;
        this.dataBuffer = dataBuffer;
        this.valueLogGroup = valueLogGroup;
    }

    /**
     * Create a new SSTable
     *
     * @param baseDir Base directory
     * @param ssTableNo SSTable No.
     * @param levelNo Level No.
     * @param options options
     * @param keyEntries Key entries
     * @param valueLogGroup Value log group
     * @return SSTable
     */
    public static SsTable create(
            Path baseDir,
            int ssTableNo,
            int levelNo,
            LsmTreeOptions options,
            List<KeyEntry> keyEntries,
            LogGroup valueLogGroup
    ) {
        if(keyEntries.isEmpty()) {
            throw new RuntimeException();
        }

        String filename = NameUtils.name(ssTableNo) + FileType.SSTABLE_FILE.suffix();
        Path filePath = Path.of(baseDir.toString(), filename);

        if(FileUtils.exist(filePath)) {
            throw new RuntimeException();
        }

        FileUtils.createFile(filePath);

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

        MetaHeader metaHeader = new MetaHeader(
                metaRegionLength,
                FileType.SSTABLE_FILE.magicNumber(),
                memTableSize,
                maxKeySize,
                keySize,
                bloomFilter.length(),
                firstIndexRegionLength,
                secondIndexRegionLength,
                dataRegionLength
        );
        // 写入 MetaRegion
        MetaRegion.writeToBuffer(metaHeader, beginKey, endKey, metaBuffer.getBuffer());

        int firstIndexRegionOffset = metaRegionLength + bloomFilter.length();
        MappedBuffer firstIndexBuffer = new MappedBuffer(
                filePath,
                firstIndexRegionOffset,
                firstIndexRegionLength
        );

        // 写入一级索引
        List<FirstIndexEntry> firstIndexEntries = new ArrayList<>();
        for(int i = 0; i < keySize; i += memTableSize) {
            KeyEntry keyEntry = keyEntries.get(i);
            byte[] key = keyEntry.key();
            firstIndexEntries.add(new FirstIndexEntry(key, i));
        }
        FirstIndexEntry.writeToBuffer(firstIndexEntries, firstIndexBuffer.getBuffer());

        int secondIndexRegionOffset = firstIndexRegionOffset + firstIndexRegionLength;
        MappedBuffer secondIndexBuffer = new MappedBuffer(
                filePath,
                secondIndexRegionOffset,
                secondIndexRegionLength
        );

        // 写入二级索引
        List<SecondIndexEntry> secondIndexEntries = new ArrayList<>();
        int beginPosition = 0;
        for(int i = 0; i < keySize; i ++) {
            KeyEntry keyEntry = keyEntries.get(i);
            byte flag = keyEntry.flag();
            byte[] key = keyEntry.key();
            int length = 24 + key.length;
            secondIndexEntries.add(new SecondIndexEntry(flag, beginPosition, length));
            // 更新 begin
            beginPosition += length;
        }
        SecondIndexEntry.writeToBuffer(secondIndexEntries, secondIndexBuffer.getBuffer());

        int dataRegionOffset = secondIndexRegionOffset + secondIndexRegionLength;
        MappedBuffer dataBuffer = new MappedBuffer(
                filePath,
                dataRegionOffset,
                dataRegionLength
        );
        KeyEntry.writeToBuffer(keyEntries, dataBuffer.getBuffer());

        return new SsTable(
                metaHeader,
                beginKey,
                endKey,
                firstIndexEntries,
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
            SecondIndexEntry index = SecondIndexEntry.from(indexMapperBuffer);
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

    public boolean bloomFilterContains(byte[] key) {
        return bloomFilter.isExist(key);
    }

    public byte[] find(byte[] key) throws DeletedException, TimeoutException {
        if(Arrays.compare(key, beginKey) < 0 || Arrays.compare(key, endKey) > 0) {
            return null;
        }

        SecondIndexRegion secondIndexRegion = findSecondIndexRegion(key);
        ByteBuffer buffer = secondIndexRegion.buffer();
        int keyAmount = secondIndexRegion.keyAmount();

        // 再查二级索引
        Pair<KeyEntry, Integer> one = binarySearch(key, keyAmount, buffer);
        int idx = one.right();
        // 当前 Key 比 Region 最后一个 Key 还要大，所以不存在当前 Key
        if(idx >= keyAmount) {
            return null;
        }

        SecondIndexEntry secondIndexEntry = findSecondIndexEntry(idx, buffer);
        KeyEntry keyEntry = findKeyEntry(secondIndexEntry);
        // 当前 Key 不存在
        if(!Arrays.equals(keyEntry.key(), key)) {
            return null;
        }

        if(secondIndexEntry.flag() == Flags.DELETED) {
            throw new DeletedException();
        }

        if(keyEntry.isTimeout()) {
            throw new TimeoutException();
        }

        int globalIndex = keyEntry.valueGlobalIndex();
        LogEntry entry = valueLogGroup.findEntry(globalIndex);

        if(entry == null) {
            throw new RuntimeException();
        }

        return entry.data();
    }

    public boolean existKey(byte[] key) throws DeletedException, TimeoutException {
        if(bloomFilter.isNotExist(key)) {
            return false;
        }

        SecondIndexRegion secondIndexRegion = findSecondIndexRegion(key);
        ByteBuffer buffer = secondIndexRegion.buffer();
        int keyAmount = secondIndexRegion.keyAmount();

        Pair<KeyEntry, Integer> one = binarySearch(key, keyAmount, buffer);
        int idx = one.right();

        if(idx >= metaHeader.keyAmount()) {
            return false;
        }

        SecondIndexEntry secondIndexEntry = findSecondIndexEntry(idx, buffer);
        KeyEntry keyEntry = findKeyEntry(secondIndexEntry);

        if(!Arrays.equals(key, keyEntry.key())) {
            return false;
        }

        if(keyEntry.isTimeout()) {
            throw new TimeoutException();
        }

        if(secondIndexEntry.flag() == Flags.DELETED) {
            throw new DeletedException();
        }

        return true;
    }

    public List<Key> rangeNext(
            byte[] beginKey,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys
    ) {
        Pair<Integer, Integer> info = findIdxBiggerThan(beginKey);
        return range(
                info.left(),
                info.right(),
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
        Pair<Integer, Integer> info = findIdxLessThan(endKey);
        return range(
                info.left(),
                info.right(),
                limit,
                deletedKeys,
                existedKeys,
                false
        );
    }

    private List<Key> range(
            int firstIndexBeginIdx,
            int secondIndexBeginIdx,
            int limit,
            HashSet<Key> deletedKeys,
            HashSet<Key> existedKeys,
            boolean isRangeNext
    ) {
        List<Key> range = new ArrayList<>();

        while (isRangeNext ? firstIndexBeginIdx < firstIndexEntries.size() : firstIndexBeginIdx > 0) {
            SecondIndexRegion secondIndexRegion = findSecondIndexRegion(firstIndexBeginIdx);
            ByteBuffer buffer = secondIndexRegion.buffer();
            int keyAmount = secondIndexRegion.keyAmount();

            while (limit > 0 && secondIndexBeginIdx < keyAmount && secondIndexBeginIdx >= 0) {
                SecondIndexEntry secondIndexEntry = findSecondIndexEntry(secondIndexBeginIdx, buffer);
                secondIndexBeginIdx = isRangeNext ? secondIndexBeginIdx + 1 : secondIndexBeginIdx - 1;

                KeyEntry keyEntry = findKeyEntry(secondIndexEntry);

                Key key = new Key(keyEntry.key());

                if(secondIndexEntry.flag() == Flags.DELETED) {
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

            firstIndexBeginIdx = isRangeNext ? firstIndexBeginIdx + 1 : firstIndexBeginIdx - 1;
            secondIndexBeginIdx = isRangeNext ? 0 : metaHeader.memTableSize() - 1;
        }

        return range;
    }

    private static int maxKeySize(int levelNo, LsmTreeOptions options) {
        return options.memTableSize()
                * (int) Math.pow(Level.LEVEL_SSTABLE_COUNT, (levelNo - Levels.LEVEL_BEGIN));
    }

    // 找第一个大于等于 dbKey 的下标
    private Pair<KeyEntry, Integer> binarySearch(byte[] key, int keyAmount, ByteBuffer buffer) {
        int begin = 0, end = keyAmount - 1, mid, idx = keyAmount;

        SecondIndexEntry midSecondIndexEntry;
        KeyEntry midKeyEntry = null;

        while(begin <= end) {
            mid = begin + ((end - begin) >> 1);
            midSecondIndexEntry = findSecondIndexEntry(mid, buffer);
            midKeyEntry = findKeyEntry(midSecondIndexEntry);

            if(Arrays.compare(key, midKeyEntry.key()) <= 0) {
                idx = mid;
                end = mid - 1;
            } else {
                begin = mid + 1;
            }
        }

        return new Pair<>(midKeyEntry, idx);
    }

    private Pair<Integer, Integer> findIdxBiggerThan(byte[] key) {
        SecondIndexRegion secondIndexRegion = findSecondIndexRegion(key);
        int keyAmount = secondIndexRegion.keyAmount();
        ByteBuffer buffer = secondIndexRegion.buffer();

        Pair<KeyEntry, Integer> result = binarySearch(key, keyAmount, buffer);
        KeyEntry midKeyEntry = result.left();
        int idx = result.right();

        int secondIndexBeginIdx = Arrays.equals(key, midKeyEntry.key()) ? idx + 1 : idx;
        int firstIndexBeginIdx = secondIndexRegion.firstIndexEntryIdx();
        return new Pair<>(firstIndexBeginIdx, secondIndexBeginIdx);
    }

    private Pair<Integer, Integer> findIdxLessThan(byte[] key) {
        SecondIndexRegion secondIndexRegion = findSecondIndexRegion(key);
        int keyAmount = secondIndexRegion.keyAmount();
        ByteBuffer buffer = secondIndexRegion.buffer();

        Pair<KeyEntry, Integer> result = binarySearch(key, keyAmount, buffer);
        KeyEntry midKeyEntry = result.left();
        int idx = result.right();

        int secondIndexBeginIdx = Arrays.equals(key, midKeyEntry.key()) ? idx - 1 : idx;
        int firstIndexBeginIdx = secondIndexRegion.firstIndexEntryIdx();
        return new Pair<>(firstIndexBeginIdx, secondIndexBeginIdx);
    }

    private SecondIndexRegion findSecondIndexRegion(byte[] key) {
        // 先查找一级索引
        int begin = 0, end = firstIndexEntries.size() - 1, mid = -1;
        FirstIndexEntry midFirstIndexEntry = null;

        while(begin <= end) {
            mid = begin + ((end - begin) >> 1);
            midFirstIndexEntry = firstIndexEntries.get(mid);

            int compareValue = Arrays.compare(key, midFirstIndexEntry.beginKey());
            if(compareValue == 0) {
                break;
            }

            if(compareValue < 0) {
                end = mid - 1;
            } else {
                begin = mid + 1;
            }
        }

        if(midFirstIndexEntry == null) {
            throw new RuntimeException();
        }

        if(Arrays.compare(key, midFirstIndexEntry.beginKey()) < 0) {
            mid = mid - 1;
        }

        return findSecondIndexRegion(mid);
    }

    private SecondIndexRegion findSecondIndexRegion(int firstIndexEntryIdx) {
        if(firstIndexEntryIdx >= firstIndexEntries.size()) {
            throw new RuntimeException();
        }

        FirstIndexEntry firstIndexEntry = firstIndexEntries.get(firstIndexEntryIdx);
        int beginPosition = firstIndexEntry.idx() * SECOND_INDEX_ENTRY_LENGTH;
        int totalKeyAmount;
        // 如果是最后一块区域
        if(firstIndexEntryIdx == firstIndexEntries.size() - 1) {
            totalKeyAmount = metaHeader.keyAmount() % metaHeader.memTableSize();
        } else {
            totalKeyAmount = metaHeader.memTableSize();
        }
        int secondIndexRegionLength = totalKeyAmount * SECOND_INDEX_ENTRY_LENGTH;
        ByteBuffer buffer = secondIndexBuffer.getBuffer().slice(beginPosition, secondIndexRegionLength);

        return new SecondIndexRegion(firstIndexEntryIdx, buffer, totalKeyAmount);
    }

    private SecondIndexEntry findSecondIndexEntry(int idx, ByteBuffer buffer) {
        buffer.position(idx * SECOND_INDEX_ENTRY_LENGTH);
        return SecondIndexEntry.from(buffer);
    }

    private KeyEntry findKeyEntry(SecondIndexEntry secondIndexEntry) {
        ByteBuffer buffer = dataBuffer.getBuffer();
        buffer.position(secondIndexEntry.begin());

        byte[] keyData = new byte[secondIndexEntry.length()];
        buffer.get(keyData);

        return KeyEntry.from(secondIndexEntry.flag(), keyData);
    }
}
