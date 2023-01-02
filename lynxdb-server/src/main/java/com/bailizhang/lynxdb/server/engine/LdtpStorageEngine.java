package com.bailizhang.lynxdb.server.engine;

import com.bailizhang.lynxdb.core.common.BytesList;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.core.utils.BufferUtils;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;
import com.bailizhang.lynxdb.server.annotations.LdtpCode;
import com.bailizhang.lynxdb.server.annotations.LdtpMethod;
import com.bailizhang.lynxdb.server.engine.affect.AffectKey;
import com.bailizhang.lynxdb.server.engine.params.QueryParams;
import com.bailizhang.lynxdb.server.engine.result.QueryResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.List;

import static com.bailizhang.lynxdb.server.annotations.LdtpCode.DB_VALUE_LIST;
import static com.bailizhang.lynxdb.server.annotations.LdtpCode.VOID;
import static com.bailizhang.lynxdb.server.annotations.LdtpMethod.*;

public class LdtpStorageEngine extends BaseStorageEngine {
    private static final Logger logger = LogManager.getLogger("LdtpStorageEngine");

    public LdtpStorageEngine() {
        super(LdtpStorageEngine.class);
    }

    @LdtpMethod(FIND_BY_KEY_CF_COLUMN)
    public QueryResult doFindByKeyCfColumn(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);

        byte[] value = find(key, columnFamily, column);

        logger.debug("Find by key: {}, columnFamily: {}, column: {}, value is: {}.",
                G.I.toString(key), G.I.toString(columnFamily), G.I.toString(column), G.I.toString(value));

        BytesList bytesList = new BytesList();

        if(value == null) {
            bytesList.appendRawByte(LdtpCode.NULL);
        } else {
            bytesList.appendRawByte(LdtpCode.BYTE_ARRAY);
            bytesList.appendRawBytes(value);
        }

        return new QueryResult(bytesList, null);
    }

    @LdtpMethod(FIND_BY_KEY_CF)
    public QueryResult doFindByKeyCf(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);

        return doFindByKeyCfColumn(key, columnFamily);
    }

    public QueryResult doFindByKeyCfColumn(byte[] key, byte[] columnFamily) {
        List<DbValue> values = find(key, columnFamily);

        logger.debug("Find by key: {}, columnFamily: {}.",
                G.I.toString(key), G.I.toString(columnFamily));

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(DB_VALUE_LIST);
        values.forEach(bytesList::append);

        return new QueryResult(bytesList, null);
    }

    @LdtpMethod(INSERT)
    public QueryResult doInsert(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);
        byte[] value = BufferUtils.getBytes(buffer);

        logger.debug("Insert key: {}, columnFamily: {}, column: {}, value: {}.",
                G.I.toString(key), G.I.toString(columnFamily), G.I.toString(column),
                G.I.toString(value));

        insert(key, columnFamily, column, value);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new AffectKey(key, columnFamily));
    }

    @LdtpMethod(DELETE)
    public QueryResult doDelete(QueryParams params) {
        byte[] data = params.content();
        ByteBuffer buffer = ByteBuffer.wrap(data);

        byte[] key = BufferUtils.getBytes(buffer);
        byte[] columnFamily = BufferUtils.getBytes(buffer);
        byte[] column = BufferUtils.getBytes(buffer);

        logger.debug("Delete key: {}, columnFamily: {}, column: {}.",
                G.I.toString(key), G.I.toString(columnFamily), G.I.toString(column));

        delete(key, columnFamily, column);

        BytesList bytesList = new BytesList();
        bytesList.appendRawByte(VOID);

        return new QueryResult(bytesList, new AffectKey(key, columnFamily));
    }
}
