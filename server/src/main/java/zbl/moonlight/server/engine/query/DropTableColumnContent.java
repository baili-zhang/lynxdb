package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.common.BytesListConvertible;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.engine.QueryParams;
import zbl.moonlight.storage.core.Column;

import java.nio.ByteBuffer;
import java.util.HashSet;

public class DropTableColumnContent implements BytesListConvertible {
    private final String table;
    private final HashSet<Column> columns;

    public DropTableColumnContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());
        table = BufferUtils.getString(buffer);

        columns = new HashSet<>();

        while(!BufferUtils.isOver(buffer)) {
            byte[] bytes = BufferUtils.getBytes(buffer);
            columns.add(new Column(bytes));
        }
    }

    public DropTableColumnContent(String table, HashSet<Column> columns) {
        this.table = table;
        this.columns = columns;
    }

    public HashSet<Column> columns() {
        return columns;
    }

    public String table() {
        return table;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.DROP_TABLE_COLUMN);
        bytesList.appendVarBytes(G.I.toBytes(table));
        columns.stream().map(Column::value).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
