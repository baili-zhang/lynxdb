package com.bailizhang.lynxdb.storage.core;

import com.bailizhang.lynxdb.core.common.BytesList;

public interface Snapshot {
    String name();
    BytesList data();
}
