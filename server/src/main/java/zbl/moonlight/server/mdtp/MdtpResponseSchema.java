package zbl.moonlight.server.mdtp;

import zbl.moonlight.core.protocol.annotations.Schema;
import zbl.moonlight.core.protocol.annotations.SchemaEntry;
import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.Parsable;

@Schema({
        /* 状态码 */
        @SchemaEntry(name = MdtpSchemaEntryName.STATUS, hasLengthSize = false, length = 1),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpSchemaEntryName.SERIAL, hasLengthSize = false, length = 4),
        /* 值 */
        @SchemaEntry(name = MdtpSchemaEntryName.VALUE, hasLengthSize = true, lengthSize = 4)
})
public interface MdtpResponseSchema extends Parsable, MSerializable {
}
