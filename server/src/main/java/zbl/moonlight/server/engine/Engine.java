package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.mdtp.*;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.server.mdtp.*;

import java.lang.reflect.Method;
import java.nio.channels.SelectionKey;
import java.util.HashMap;

public abstract class Engine extends Executor {
    private static final Logger logger = LogManager.getLogger("Engine");
    /* 方法的code与方法处理函数之间的映射 */
    private final HashMap<Byte, Method> methodMap = new HashMap<>();
    private final EventBus eventBus;
    protected final Class<? extends MSerializable> schemaClass = MdtpResponseSchema.class;

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
             NioReadableEvent request = (NioReadableEvent) event.value();
             NioWritableEvent response = exec(request);
             /* selectionKey为null时，event为读取二进制日志文件的客户端请求，不需要写回 */
             if(request.selectionKey() != null) {
                 eventBus.offer(new Event(EventType.CLIENT_RESPONSE, response));
             }
        }
    }

    private NioWritableEvent exec(NioReadableEvent event) {
        byte mdtpMethod = (new MdtpRequest(event.reader())).method();
        String methodName = MdtpMethod.getMethodName(mdtpMethod);
        Method method = methodMap.get(mdtpMethod);

        if(method == null || methodName == null) {
            return errorResponse(event);
        }

        try {
            logger.debug("Invoke method [{}].", method.getName());
            return (NioWritableEvent) method.invoke(this, event);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return errorResponse(event);
    }

    protected NioWritableEvent buildMdtpResponseEvent(SelectionKey key,
                                                      byte status,
                                                      byte[] serial,
                                                      byte[] value) {
        WritableMdtpResponse response = new NioWriter(schemaClass);
        response.mapPut(MdtpSchemaEntryName.STATUS, new byte[]{status});
        response.mapPut(MdtpSchemaEntryName.SERIAL, serial);
        response.mapPut(MdtpSchemaEntryName.VALUE, value == null ? new byte[0] : value);
        response.serialize();
        return new NioWritableEvent(key, response);
    }

    protected NioWritableEvent buildMdtpResponseEvent(SelectionKey key,
                                                      byte status,
                                                      byte[] serial) {
        return buildMdtpResponseEvent(key, status, serial, null);
    }

    /* 处理请求错误的情况 */
    private NioWritableEvent errorResponse(NioReadableEvent event) {
        MdtpRequest request = (MdtpRequest) event.value();
        return buildMdtpResponseEvent(event.selectionKey(), ResponseStatus.ERROR, request.serial());
    }
}
