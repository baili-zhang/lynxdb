package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.mdtp.MdtpMethod;
import zbl.moonlight.server.mdtp.MdtpRequest;
import zbl.moonlight.server.mdtp.ResponseStatus;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.engine.Engine;

public class SimpleCache extends Engine {
    private final SimpleLRU<String, byte[]> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(ServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.GET)
    public NioWritableEvent doGet(NioReadableEvent event) {
        MdtpRequest request = (MdtpRequest) event.value();
        byte[] value = cache.get(new String(request.key()));
        byte status = value == null ? ResponseStatus.VALUE_NOT_EXIST : ResponseStatus.VALUE_EXIST;
        return buildMdtpResponseEvent(event.selectionKey(), status, request.serial(), value);
    }

    @MethodMapping(MdtpMethod.SET)
    public NioWritableEvent doSet(NioReadableEvent event) {
        MdtpRequest request = (MdtpRequest) event.value();
        cache.put(new String(request.key()), request.value());
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.DELETE)
    public NioWritableEvent doDelete(NioReadableEvent event) {
        MdtpRequest request = (MdtpRequest) event.value();
        cache.remove(new String(request.key()));
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.PING)
    public NioWritableEvent doPing(NioReadableEvent event) {
        MdtpRequest request = (MdtpRequest) event.value();
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.PONG, request.serial());
    }
}
