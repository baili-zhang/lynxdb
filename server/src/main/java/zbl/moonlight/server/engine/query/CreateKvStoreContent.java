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

public class CreateKvStoreContent implements BytesListConvertible {
    private final List<String> kvstores;

    public CreateKvStoreContent(QueryParams params) {
        ByteBuffer buffer = ByteBuffer.wrap(params.content());

        kvstores = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            kvstores.add(BufferUtils.getString(buffer));
        }
    }

    public CreateKvStoreContent(List<String> kvstores) {
        this.kvstores = kvstores;
    }

    public List<String> kvstores() {
        return kvstores;
    }

    @Override
    public BytesList toBytesList() {
        BytesList bytesList = new BytesList();

        bytesList.appendRawByte(MdtpMethod.CREATE_KV_STORE);
        kvstores.stream().map(G.I::toBytes).forEach(bytesList::appendVarBytes);

        return bytesList;
    }
}
