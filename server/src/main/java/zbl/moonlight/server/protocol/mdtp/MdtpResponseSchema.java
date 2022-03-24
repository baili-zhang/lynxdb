package zbl.moonlight.server.protocol.mdtp;

import zbl.moonlight.server.protocol.annotations.Schema;
import zbl.moonlight.server.protocol.annotations.SchemaEntry;
import zbl.moonlight.server.protocol.common.MSerializable;
import zbl.moonlight.server.protocol.common.Parsable;

@Schema({
        /* 状态码 */
        @SchemaEntry(name = MdtpSchema.STATUS, hasLengthSize = false, length = 1),
        /* 请求序列号 */
        @SchemaEntry(name = MdtpSchema.SERIAL, hasLengthSize = false, length = 4),
        /* 值 */
        @SchemaEntry(name = MdtpSchema.VALUE, hasLengthSize = true, lengthSize = 4)
})
public interface MdtpResponseSchema extends Parsable, MSerializable {
}
