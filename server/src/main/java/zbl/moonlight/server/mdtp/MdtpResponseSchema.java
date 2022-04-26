package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        /* 状态码 */
        @SchemaEntry(name = MdtpResponseSchema.STATUS, type = SchemaEntryType.BYTE, order = 101),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpResponseSchema.SERIAL, type = SchemaEntryType.INT, order = 102),
        /* 值 */
        @SchemaEntry(name = MdtpResponseSchema.VALUE, type = SchemaEntryType.STRING, order = 103)
})
public interface MdtpResponseSchema extends SocketSchema {
    /* 请求序列号 */
    String SERIAL = "MdtpResponseSchema.SERIAL";
    /* 值 */
    String VALUE = "MdtpResponseSchema.VALUE";
    /* 响应状态码 */
    String STATUS = "MdtpResponseSchema.STATUS";
}
