package com.bailizhang.lynxdb.raft.result;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.BytesListConvertible;

import java.nio.ByteBuffer;

import static com.bailizhang.lynxdb.raft.result.RaftResult.INSTALL_SNAPSHOT_RESULT;

public record InstallSnapshotResult(
        int term
) implements BytesListConvertible {
    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(INSTALL_SNAPSHOT_RESULT);
        bytesList.appendRawInt(term);

        return bytesList;
    }

    public static InstallSnapshotResult from(ByteBuffer buffer) {
        int term = buffer.get();

        return new InstallSnapshotResult(term);
    }
}
