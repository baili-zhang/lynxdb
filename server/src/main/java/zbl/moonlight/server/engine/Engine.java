package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.server.eventbus.EventBus;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.server.executor.Executor;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.lang.reflect.Method;
import java.util.HashMap;

public abstract class Engine extends Executor<Event<?>> {
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
             Event<?> event = pollSleep();
             if(event == null) {
                 continue;
             }
             MdtpRequest request = (MdtpRequest) event.getValue();
             MdtpResponse response = exec(request);
             /* selectionKey为null时，event为读取二进制日志文件的客户端请求，不需要写回 */
             if(event.getSelectionKey() != null) {
                 eventBus.offer(new Event<>(EventType.CLIENT_RESPONSE, event.getSelectionKey(), response));
             }
        }
    }

    private MdtpResponse exec(MdtpRequest mdtpRequest) {
        String methodName = MdtpMethod.getMethodName(mdtpRequest.getMethod());
        Method method = methodMap.get(mdtpRequest.getMethod());

        if(method == null || methodName == null) {
            return errorResponse(mdtpRequest);
        }

        try {
            return (MdtpResponse) method.invoke(this, mdtpRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return errorResponse(mdtpRequest);
    }

    private MdtpResponse errorResponse(MdtpRequest mdtpRequest) {
        MdtpResponse response = new MdtpResponse(mdtpRequest.getIdentifier());
        response.setError();
        return response;
    }
}
