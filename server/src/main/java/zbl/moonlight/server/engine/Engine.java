package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.mdtp.*;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.core.protocol.common.ReadableEvent;
import zbl.moonlight.core.protocol.common.WritableEvent;

import java.lang.reflect.Method;
import java.nio.channels.SelectionKey;
import java.util.HashMap;

public abstract class Engine extends Executor {
    private static final Logger logger = LogManager.getLogger("Engine");
    /* 方法的code与方法处理函数之间的映射 */
    private final HashMap<Byte, Method> methodMap = new HashMap<>();
    private final EventBus eventBus;

    protected Engine() {
        eventBus = ServerContext.getInstance().getEventBus();
        Method[] methods = this.getClass().getDeclaredMethods();
        for(Method method : methods) {
            MethodMapping methodMapping = method.getAnnotation(MethodMapping.class);
            if(methodMapping != null) {
                methodMap.put(methodMapping.value(), method);
                String name = MdtpMethod.getMethodName(methodMapping.value());
                logger.debug("{} has mapped to {}", name, method);
            }
        }
    }

    @Override
    public final void run() {
        while (true) {
             Event event = pollSleep();
             if(event == null) {
                 continue;
             }
             ReadableEvent request = (ReadableEvent) event.value();
             WritableEvent response = exec(request);
             /* selectionKey为null时，event为读取二进制日志文件的客户端请求，不需要写回 */
             if(request.selectionKey() != null) {
                 eventBus.offer(new Event(EventType.CLIENT_RESPONSE, response));
             }
        }
    }

    private WritableEvent exec(ReadableEvent event) {
        byte mdtpMethod = ((ReadableMdtpRequest) event.readable()).method();
        String methodName = MdtpMethod.getMethodName(mdtpMethod);
        Method method = methodMap.get(mdtpMethod);

        if(method == null || methodName == null) {
            return errorResponse(event);
        }

        try {
            logger.debug("Invoke method [{}].", method.getName());
            return (WritableEvent) method.invoke(this, event);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return errorResponse(event);
    }

    protected WritableEvent buildMdtpResponseEvent(SelectionKey key,
                                                   byte status,
                                                   byte[] serial,
                                                   byte[] value) {
        WritableMdtpResponse response = new WritableMdtpResponse();
        response.put(MdtpSchema.STATUS, new byte[]{status});
        response.put(MdtpSchema.SERIAL, serial);
        response.put(MdtpSchema.VALUE, value == null ? new byte[0] : value);
        response.serialize();
        return new WritableEvent(key, response);
    }

    protected WritableEvent buildMdtpResponseEvent(SelectionKey key,
                                                   byte status,
                                                   byte[] serial) {
        return buildMdtpResponseEvent(key, status, serial, null);
    }

    /* 处理请求错误的情况 */
    private WritableEvent errorResponse(ReadableEvent event) {
        ReadableMdtpRequest request = (ReadableMdtpRequest) event.readable();
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.ERROR, request.serial());
    }
}
