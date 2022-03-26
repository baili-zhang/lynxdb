package zbl.moonlight.server.mdtp.server;

import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.socket.SocketServerConfig;
import zbl.moonlight.core.socket.SocketServer;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;
import zbl.moonlight.server.mdtp.MdtpRequestSchema;

public class MdtpSocketServer extends SocketServer {
    private static final ServerContext serverContext = ServerContext.getInstance();
    private static final Configuration config = serverContext.getConfiguration();
    private static final SocketServerConfig socketServerConfig;
    private static final String ioThreadNamePrefix = "Server-IO-";

    static  {
        socketServerConfig = new SocketServerConfig(config.getPort(), serverContext.getEventBus(),
                EventType.ENGINE_REQUEST, MdtpRequestSchema.class);

        socketServerConfig
                .coreSize(config.getIoThreadCorePoolSize())
                .maxPoolSize(config.getIoThreadMaxPoolSize())
                .keepAliveTime(config.getIoThreadKeepAliveTime())
                .blockingQueueSize(config.getIoThreadBlockingQueueSize())
                .ioThreadNamePrefix(ioThreadNamePrefix)
                .backlog(config.getBacklog());
    }

    public MdtpSocketServer() {
        super(socketServerConfig);
    }
}
