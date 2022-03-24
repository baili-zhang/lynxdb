package zbl.moonlight.core.protocol.mdtp;

import zbl.moonlight.core.protocol.annotations.Schema;
import zbl.moonlight.core.protocol.annotations.SchemaEntry;
import zbl.moonlight.core.protocol.common.MSerializable;
import zbl.moonlight.core.protocol.common.Parsable;

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
