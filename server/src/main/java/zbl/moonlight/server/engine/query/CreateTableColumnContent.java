package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.common.BytesListConvertible;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Column;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateTableColumnContent implements BytesListConvertible {
    private final String table;
    private final List<byte[]> columns;

    public CreateTableColumnContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        columns = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            columns.add(BufferUtils.getBytes(buffer));
        }
    }

    public CreateTableColumnContent(String table, List<byte[]> columns) {
        this.table = table;
        this.columns = columns;
    }

    public List<byte[]> columns() {
        return columns;
    }

    public String table() {
        return table;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.CREATE_TABLE_COLUMN);
        bytesList.appendVarBytes(G.I.toBytes(table));
        columns.forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
