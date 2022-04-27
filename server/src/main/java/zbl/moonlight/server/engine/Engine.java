package zbl.moonlight.server.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.eventbus.EventType;
import zbl.moonlight.core.protocol.MSerializable;
import zbl.moonlight.core.protocol.nio.NioReader;
import zbl.moonlight.core.protocol.nio.NioWriter;
import zbl.moonlight.core.socket.interfaces.SocketState;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.config.RunningMode;
import zbl.moonlight.server.mdtp.server.MdtpServerContext;
import zbl.moonlight.server.eventbus.*;
import zbl.moonlight.server.eventbus.Event;
import zbl.moonlight.core.executor.Executor;
import zbl.moonlight.server.mdtp.*;
import zbl.moonlight.server.raft.RaftRole;
import zbl.moonlight.server.raft.RaftState;
import zbl.moonlight.server.raft.log.RaftLogEntry;

import java.lang.reflect.Method;
import java.nio.channels.SelectionKey;
import java.util.HashMap;

public abstract class Engine extends Executor {
    private static final Logger logger = LogManager.getLogger("Engine");
    /* 方法的code与方法处理函数之间的映射 */
    private final HashMap<Byte, Method> methodMap = new HashMap<>();
    private final EventBus eventBus;
    private final Configuration config;
    /* Raft集群节点的相关状态 */
    protected final RaftState raftState;
    protected final Class<? extends MSerializable> schemaClass = MdtpResponseSchema.class;

    protected Engine() {
        MdtpServerContext context = MdtpServerContext.getInstance();
        eventBus = context.getEventBus();
        config = context.getConfiguration();
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
            Event event = blockPoll();
            if(event == null) {
                continue;
            }

            /* TODO:恢复数据，应该在完全启动之前进行数据的恢复 */
            if(event.value() instanceof RaftLogEntry logEntry) {
                System.out.println(logEntry);
                continue;
            }

            NioReader reader = (NioReader) event.value();
            MdtpRequest request = new MdtpRequest(reader);
            byte mdtpMethod = request.method();

            if(config.getRunningMode().equals(RunningMode.CLUSTER)
                    && event.type().equals(EventType.CLIENT_REQUEST)
                    && (mdtpMethod == MdtpMethod.SET || mdtpMethod == MdtpMethod.DELETE)) {
                sendToRaftClient(reader);
                continue;
            }

            NioWriter writer = exec(request);
            eventBus.offer(new Event(EventType.CLIENT_RESPONSE, writer));
        }
    }

    private void sendToRaftClient(NioReader reader) {
        eventBus.offer(new Event(EventType.CLUSTER_REQUEST, reader));
    }

    private NioWriter exec(MdtpRequest mdtpRequest) {
        byte mdtpMethod = mdtpRequest.method();

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
            return errorResponse(mdtpRequest);
        }

        try {
            logger.debug("Received MDTP request is: {}", mdtpRequest);
            return (NioWriter) method.invoke(this, mdtpRequest);
        } catch (Exception e) {
            e.printStackTrace();
        }

        return errorResponse(mdtpRequest);
    }

    protected NioWriter buildMdtpResponseEvent(SelectionKey key,
                                                      byte status,
                                                      byte[] serial,
                                                      byte[] value) {
        NioWriter writer = new NioWriter(schemaClass, key);
        writer.mapPut(SocketSchema.SOCKET_STATUS, new byte[]{SocketState.STAY_CONNECTED});
        writer.mapPut(MdtpResponseSchema.STATUS, new byte[]{status});
        writer.mapPut(MdtpResponseSchema.SERIAL, serial);
        writer.mapPut(MdtpResponseSchema.VALUE, value == null ? new byte[0] : value);
        return writer;
    }

    protected NioWriter buildMdtpResponseEvent(SelectionKey key,
                                                      byte status,
                                                      byte[] serial) {
        return buildMdtpResponseEvent(key, status, serial, null);
    }

    /* 处理请求错误的情况 */
    private NioWriter errorResponse(MdtpRequest request) {
        return buildMdtpResponseEvent(request.selectionKey(), ResponseStatus.ERROR, request.serial());
    }
}
