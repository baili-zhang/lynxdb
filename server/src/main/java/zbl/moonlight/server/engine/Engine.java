package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.mdtp.*;

import java.lang.reflect.Method;
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
             MdtpRequestEvent request = (MdtpRequestEvent) event.value();
             MdtpResponseEvent response = exec(request);
             /* selectionKey为null时，event为读取二进制日志文件的客户端请求，不需要写回 */
             if(request.selectionKey() != null) {
                 eventBus.offer(new Event(EventType.CLIENT_RESPONSE, response));
             }
        }
    }

    private MdtpResponseEvent exec(MdtpRequestEvent event) {
        byte mdtpMethod = event.request().method();
        String methodName = MdtpMethod.getMethodName(mdtpMethod);
        Method method = methodMap.get(mdtpMethod);

        if(method == null || methodName == null) {
            return errorResponse(event);
        }

        try {
            return (MdtpResponseEvent) method.invoke(this, event);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return errorResponse(event);
    }

    /* 处理请求错误的情况 */
    private MdtpResponseEvent errorResponse(MdtpRequestEvent event) {
        ReadableMdtpRequest request = event.request();
        WritableMdtpResponse response = new WritableMdtpResponse();
        response.put(MdtpSchema.STATUS, new byte[]{ResponseStatus.ERROR});
        response.put(MdtpSchema.SERIAL, request.serial());
        return new MdtpResponseEvent(event.selectionKey(), response);
    }
}
