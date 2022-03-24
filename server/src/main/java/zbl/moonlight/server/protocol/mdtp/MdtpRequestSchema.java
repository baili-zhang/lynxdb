package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.annotations.Schema;
import zbl.moonlight.server.protocol.annotations.SchemaEntry;
import zbl.moonlight.server.protocol.common.MSerializable;
import zbl.moonlight.server.protocol.common.Parsable;

@Schema({
        /* 请求方法 */
        @SchemaEntry(name = MdtpSchema.METHOD, hasLengthSize = false, length = 1),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpSchema.SERIAL, hasLengthSize = false, length = 4),
        /* 键 */
        @SchemaEntry(name = MdtpSchema.KEY, hasLengthSize = true, lengthSize = 1),
        /* 值 */
        @SchemaEntry(name = MdtpSchema.VALUE, hasLengthSize = true, lengthSize = 4)
})
public interface MdtpRequestSchema extends Parsable, MSerializable {
}
