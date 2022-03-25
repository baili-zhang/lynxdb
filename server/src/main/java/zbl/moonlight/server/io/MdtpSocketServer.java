package zbl.moonlight.server.io;

import zbl.moonlight.core.executor.EventType;
import zbl.moonlight.core.protocol.mdtp.ReadableMdtpRequest;
import zbl.moonlight.core.socket.SocketServer;
import zbl.moonlight.server.config.Configuration;
import zbl.moonlight.server.context.ServerContext;

public class MdtpSocketServer extends SocketServer {
    private static final ServerContext serverContext = ServerContext.getInstance();
    private static final Configuration config = serverContext.getConfiguration();
    private static final String ioThreadNamePrefix = "Server-IO-";

    public MdtpSocketServer() {
        super(config.getIoThreadCorePoolSize(), config.getIoThreadMaxPoolSize(),
                config.getIoThreadKeepAliveTime(), config.getIoThreadBlockingQueueSize(),
                config.getPort(), config.getBacklog(), serverContext.getEventBus(),
                EventType.ENGINE_REQUEST, ioThreadNamePrefix, ReadableMdtpRequest.class);
    }
}
