package com.bailizhang.lynxdb.storage.rocks;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.storage.core.Snapshot;

public record RocksSnapshot(String name, BytesList data) implements Snapshot {
}
