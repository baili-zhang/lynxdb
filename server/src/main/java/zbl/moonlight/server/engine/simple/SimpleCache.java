package zbl.moonlight.server.engine.simple;

import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.engine.MethodMapping;
import zbl.moonlight.server.eventbus.MdtpRequestEvent;
import zbl.moonlight.server.eventbus.MdtpResponseEvent;
import zbl.moonlight.server.protocol.mdtp.*;
import zbl.moonlight.server.engine.Engine;

public class SimpleCache extends Engine {
    private final SimpleLRU<String, byte[]> cache;

    public SimpleCache() {
        cache = new SimpleLRU<>(ServerContext.getInstance().getConfiguration().getCacheCapacity());
    }

    @MethodMapping(MdtpMethod.GET)
    public MdtpResponseEvent doGet(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        byte[] value = cache.get(new String(request.key()));
        byte status = value == null ? ResponseStatus.VALUE_NOT_EXIST : ResponseStatus.VALUE_EXIST;
        return buildMdtpResponseEvent(event.selectionKey(), status, request.serial(), value);
    }

    @MethodMapping(MdtpMethod.SET)
    public MdtpResponseEvent doSet(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        cache.put(new String(request.key()), request.value());
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.DELETE)
    public MdtpResponseEvent doDelete(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        cache.remove(new String(request.key()));
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.SUCCESS_NO_VALUE, request.serial());
    }

    @MethodMapping(MdtpMethod.PING)
    public MdtpResponseEvent doPing(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.PONG, request.serial());
    }
}
