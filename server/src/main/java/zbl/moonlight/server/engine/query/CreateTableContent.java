package zbl.moonlight.server.engine.query;

import zbl.moonlight.core.common.BytesList;
import zbl.moonlight.core.common.BytesListConvertible;
import zbl.moonlight.core.common.G;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.annotations.MdtpMethod;
import zbl.moonlight.server.engine.QueryParams;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class CreateTableContent implements BytesListConvertible {
    private final List<String> tables;

    public CreateTableContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        tables = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            tables.add(BufferUtils.getString(buffer));
        }
    }

    public CreateTableContent(List<String> tables) {
        this.tables = tables;
    }

    public List<String> tables() {
        return tables;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.CREATE_TABLE);
        tables.stream().map(G.I::toBytes).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
