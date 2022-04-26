package zbl.moonlight.server.mdtp.server;

import zbl.moonlight.core.socket.server.SocketServerConfig;
import zbl.moonlight.core.socket.server.SocketServer;
import zbl.moonlight.server.config.Configuration;

import java.io.IOException;

public class MdtpSocketServer extends SocketServer {
    private static final MdtpServerContext MDTP_SERVER_CONTEXT = MdtpServerContext.getInstance();
    private static final Configuration config = MDTP_SERVER_CONTEXT.getConfiguration();
    private static final SocketServerConfig socketServerConfig;
    private static final String ioThreadNamePrefix = "Server-IO-";

    static {
        socketServerConfig = new SocketServerConfig(config.getPort());

        socketServerConfig
                .coreSize(config.getIoThreadCorePoolSize())
                .maxPoolSize(config.getIoThreadMaxPoolSize())
                .keepAliveTime(config.getIoThreadKeepAliveTime())
                .blockingQueueSize(config.getIoThreadBlockingQueueSize())
                .ioThreadNamePrefix(ioThreadNamePrefix)
                .backlog(config.getBacklog());
    }

    public MdtpSocketServer() throws IOException {
        super(socketServerConfig);
    }
}
