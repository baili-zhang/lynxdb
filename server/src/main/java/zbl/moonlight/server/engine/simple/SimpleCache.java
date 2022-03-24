package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.eventbus.MdtpRequestEvent;
import zbl.moonlight.server.eventbus.MdtpResponseEvent;
import zbl.moonlight.server.protocol.mdtp.*;
import zbl.moonlight.server.engine.Engine;

import java.nio.charset.StandardCharsets;

public class SimpleCache extends Engine {
    private final SimpleLRU<byte[], byte[]> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(ServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.GET)
    public MdtpResponseEvent doGet(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        byte[] value = cache.get(request.key());
        WritableMdtpResponse response = new WritableMdtpResponse();
        if(value == null) {
            response.put(MdtpSchema.STATUS, new byte[]{ResponseStatus.VALUE_NOT_EXIST});
        } else {
            response.put(MdtpSchema.STATUS, new byte[]{ResponseStatus.VALUE_EXIST});
            response.put(MdtpSchema.VALUE, value);
        }
        response.put(MdtpSchema.SERIAL, request.serial());
        return new MdtpResponseEvent(event.selectionKey(), response);
    }

    @MethodMapping(MdtpMethod.SET)
    public MdtpResponseEvent doSet(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        cache.put(request.key(), request.value());
        WritableMdtpResponse response = new WritableMdtpResponse();
        response.put(MdtpSchema.STATUS, new byte[]{ResponseStatus.SUCCESS_NO_VALUE});
        response.put(MdtpSchema.SERIAL, request.serial());
        return new MdtpResponseEvent(event.selectionKey(), response);
    }

    @MethodMapping(MdtpMethod.DELETE)
    public MdtpResponseEvent doDelete(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        cache.remove(request.key());
        WritableMdtpResponse response = new WritableMdtpResponse();
        response.put(MdtpSchema.STATUS, new byte[]{ResponseStatus.SUCCESS_NO_VALUE});
        response.put(MdtpSchema.SERIAL, request.serial());
        return new MdtpResponseEvent(event.selectionKey(), response);
    }

    @MethodMapping(MdtpMethod.PING)
    public MdtpResponseEvent doPing(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        WritableMdtpResponse response = new WritableMdtpResponse();
        response.put(MdtpSchema.STATUS, new byte[]{ResponseStatus.VALUE_EXIST});
        response.put(MdtpSchema.VALUE, "PONG".getBytes(StandardCharsets.UTF_8));
        response.put(MdtpSchema.SERIAL, request.serial());
        return new MdtpResponseEvent(event.selectionKey(), response);
    }
}
