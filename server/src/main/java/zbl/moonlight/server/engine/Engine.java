package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.protocol.nio.SocketState;
import zbl.moonlight.core.socket.SocketSchemaEntryName;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.core.executor.Event;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.server.mdtp.*;
import zbl.moonlight.server.raft.RaftRole;
import zbl.moonlight.server.raft.RaftState;

import java.lang.reflect.Method;
import java.nio.channels.SelectionKey;
import java.util.HashMap;

public abstract class Engine extends Executor {
    private static final Logger logger = LogManager.getLogger("Engine");
    /* 方法的code与方法处理函数之间的映射 */
    private final HashMap<Byte, Method> methodMap = new HashMap<>();
    private final EventBus eventBus;
    /* Raft集群节点的相关状态 */
    protected final RaftState raftState;
    protected final Class<? extends MSerializable> schemaClass = MdtpResponseSchema.class;

    protected Engine() {
        MdtpServerContext context = MdtpServerContext.getInstance();
        eventBus = context.getEventBus();
        raftState = context.getRaftState();

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
             NioReader reader = (NioReader) event.value();
             NioWriter writer = exec(reader);
             eventBus.offer(new Event(EventType.CLIENT_RESPONSE, writer));
        }
    }

    private NioWriter exec(NioReader reader) {
        MdtpRequest mdtpRequest = new MdtpRequest(reader);
        byte mdtpMethod = (new MdtpRequest(reader)).method();

        /* 接收到心跳，更新Raft的计时器时间 */
        if(mdtpMethod == MdtpMethod.APPEND_ENTRIES
                || (mdtpMethod == MdtpMethod.REQUEST_VOTE
                    && raftState.getRaftRole() == RaftRole.Follower)) {
            raftState.setTimeoutTimeMillis(System.currentTimeMillis());
            logger.debug("[#{}] Reset timeout timer.", raftState.getRaftRole());
        }

        String methodName = MdtpMethod.getMethodName(mdtpMethod);
        Method method = methodMap.get(mdtpMethod);

        if(method == null || methodName == null) {
            return errorResponse(reader);
        }

        try {
            logger.debug("Received MDTP request is: {}", mdtpRequest);
            return (NioWriter) method.invoke(this, reader);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return errorResponse(reader);
    }

    protected NioWriter buildMdtpResponseEvent(SelectionKey key,
                                                      byte status,
                                                      byte[] serial,
                                                      byte[] value) {
        NioWriter writer = new NioWriter(schemaClass, key);
        writer.mapPut(SocketSchemaEntryName.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpSchemaEntryName.STATUS, new byte[]{status});
        writer.mapPut(MdtpSchemaEntryName.SERIAL, serial);
        writer.mapPut(MdtpSchemaEntryName.VALUE, value == null ? new byte[0] : value);
        return writer;
    }

    protected NioWriter buildMdtpResponseEvent(SelectionKey key,
                                                      byte status,
                                                      byte[] serial) {
        return buildMdtpResponseEvent(key, status, serial, null);
    }

    /* 处理请求错误的情况 */
    private NioWriter errorResponse(NioReader reader) {
        MdtpRequest request = new MdtpRequest(reader);
        return buildMdtpResponseEvent(reader.getSelectionKey(), ResponseStatus.ERROR, request.serial());
    }
}
