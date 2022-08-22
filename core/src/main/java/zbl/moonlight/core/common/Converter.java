package zbl.moonlight.core.common;

import java.nio.charset.Charset;

public record Converter(Charset charset) {
    public byte[] toBytes(String src) {
        if(src == null) {
            return new byte[0];
        }

        return src.getBytes(charset);
    }
}
