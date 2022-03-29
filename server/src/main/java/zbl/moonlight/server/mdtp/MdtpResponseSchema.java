package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;
import zbl.moonlight.core.socket.SocketSchema;

@Schema({
        /* 状态码 */
        @SchemaEntry(name = MdtpSchemaEntryName.STATUS, type = SchemaEntryType.BYTE, order = 101),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpSchemaEntryName.SERIAL, type = SchemaEntryType.INT, order = 102),
        /* 值 */
        @SchemaEntry(name = MdtpSchemaEntryName.VALUE, type = SchemaEntryType.STRING, order = 103)
})
public interface MdtpResponseSchema extends SocketSchema {
}
