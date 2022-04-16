package zbl.moonlight.server.mdtp.server;

import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.socket.SocketServerConfig;
import zbl.moonlight.core.socket.SocketServer;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.mdtp.MdtpRequestSchema;

public class MdtpSocketServer extends SocketServer {
    private static final MdtpServerContext MDTP_SERVER_CONTEXT = MdtpServerContext.getInstance();
    private static final Configuration config = MDTP_SERVER_CONTEXT.getConfiguration();
    private static final SocketServerConfig socketServerConfig;
    private static final String ioThreadNamePrefix = "Server-IO-";

    static  {
        socketServerConfig = new SocketServerConfig(config.getPort(), MDTP_SERVER_CONTEXT.getEventBus(),
                EventType.CLIENT_REQUEST, MdtpRequestSchema.class);

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
