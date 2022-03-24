package zbl.moonlight.core.protocol.mdtp;

import zbl.moonlight.core.protocol.annotations.Schema;
import zbl.moonlight.core.protocol.annotations.SchemaEntry;
import zbl.moonlight.core.protocol.common.MSerializable;
import zbl.moonlight.core.protocol.common.Parsable;

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
