package zbl.moonlight.core.socket;

import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;
import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        /* 是否保持连接 */
        @SchemaEntry(name = SocketSchema.SOCKET_STATUS, type = SchemaEntryType.BYTE, order = 0),
})
public interface SocketSchema extends MSerializable, Parsable {
    String SOCKET_STATUS = "SocketSchema.SOCKET_STATUS";
}
