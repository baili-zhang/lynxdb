package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.annotations.Schema;
import zbl.moonlight.core.protocol.annotations.SchemaEntry;
import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;

@Schema({
        /* 请求方法 */
        @SchemaEntry(name = MdtpSchemaEntryName.METHOD, hasLengthSize = false, length = 1),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpSchemaEntryName.SERIAL, hasLengthSize = false, length = 4),
        /* 键 */
        @SchemaEntry(name = MdtpSchemaEntryName.KEY, hasLengthSize = true, lengthSize = 1),
        /* 值 */
        @SchemaEntry(name = MdtpSchemaEntryName.VALUE, hasLengthSize = true, lengthSize = 4)
})
public interface MdtpRequestSchema extends Parsable, MSerializable {
}
