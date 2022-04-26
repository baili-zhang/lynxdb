package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.schema.Schema;
import zbl.moonlight.core.protocol.schema.SchemaEntry;
import zbl.moonlight.core.protocol.schema.SchemaEntryType;

@Schema({
        /* 请求方法 */
        @SchemaEntry(name = MdtpRequestSchema.METHOD, type = SchemaEntryType.BYTE, order = 101),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpRequestSchema.SERIAL, type = SchemaEntryType.INT, order = 102),
        /* 键 */
        @SchemaEntry(name = MdtpRequestSchema.KEY, type = SchemaEntryType.STRING, order = 103),
        /* 值 */
        @SchemaEntry(name = MdtpRequestSchema.VALUE, type = SchemaEntryType.STRING, order = 104)
})
public interface MdtpRequestSchema extends SocketSchema {
    /* 请求方法 */
    String METHOD = "MdtpRequestSchema.METHOD";
    /* 请求序列号 */
    String SERIAL = "MdtpRequestSchema.SERIAL";
    /* 键 */
    String KEY = "MdtpRequestSchema.KEY";
    /* 值 */
    String VALUE = "MdtpRequestSchema.VALUE";
}
