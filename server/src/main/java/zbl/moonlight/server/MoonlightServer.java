package zbl.moonlight.server;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.io.ChannelSelector;
import zbl.moonlight.server.config.Configuration;

public class MoonlightServer {
    private static final Configuration config = new Configuration();
    private static final Logger logger = LogManager.getLogger("MoonlightServer");

    private static final int PORT = config.getPort();

    public static void main(String[] args) {
        logger.info("moonlight server is running, listening at {}:{}.", "127.0.0.1", PORT);
        new Thread(new ChannelSelector(PORT), "acceptor").start();
    }
}
