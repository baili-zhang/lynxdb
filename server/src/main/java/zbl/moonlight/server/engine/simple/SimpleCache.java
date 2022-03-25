package zbl.moonlight.server.engine.simple;

import zbl.moonlight.core.protocol.mdtp.MdtpMethod;
import zbl.moonlight.core.protocol.mdtp.ReadableMdtpRequest;
import zbl.moonlight.core.protocol.mdtp.ResponseStatus;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.core.protocol.common.ReadableEvent;
import zbl.moonlight.core.protocol.common.WritableEvent;
import zbl.moonlight.server.engine.Engine;

public class SimpleCache extends Engine {
    private final SimpleLRU<String, byte[]> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(ServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.GET)
    public WritableEvent doGet(ReadableEvent event) {
        ReadableMdtpRequest request = (ReadableMdtpRequest) event.readable();
        byte[] value = cache.get(new String(request.key()));
        byte status = value == null ? ResponseStatus.VALUE_NOT_EXIST : ResponseStatus.VALUE_EXIST;
        return buildMdtpResponseEvent(event.selectionKey(), status, request.serial(), value);
    }

    @MethodMapping(MdtpMethod.SET)
    public WritableEvent doSet(ReadableEvent event) {
        ReadableMdtpRequest request = (ReadableMdtpRequest) event.readable();
        cache.put(new String(request.key()), request.value());
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.DELETE)
    public WritableEvent doDelete(ReadableEvent event) {
        ReadableMdtpRequest request = (ReadableMdtpRequest) event.readable();
        cache.remove(new String(request.key()));
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.PING)
    public WritableEvent doPing(ReadableEvent event) {
        ReadableMdtpRequest request = (ReadableMdtpRequest) event.readable();
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.PONG, request.serial());
    }
}
