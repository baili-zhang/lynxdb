package zbl.moonlight.server.io.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.command.Command;
import zbl.moonlight.server.command.CommandFactory;
import zbl.moonlight.server.io.EventHandler;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadEventHandler extends EventHandler {
    private static final Logger logger = LogManager.getLogger("ReadEventHandler");

    public ReadEventHandler(SelectionKey selectionKey) {
        super(selectionKey);
    }

    @Override
    public void run() {
        ByteBuffer byteBuffer = (ByteBuffer) selectionKey.attachment();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();

        try {
            int n = socketChannel.read(byteBuffer);
            if(n == -1) {
                return;
            }
            byteBuffer.flip();

            String commandLine = new String(byteBuffer.array(), 0, n).trim();
            logger.info("command line is \"{}\", read from address {}:{}.", commandLine, hostName, port);

            String result = execCommand(commandLine);
            byteBuffer.clear();
            byteBuffer.put(result.getBytes());
        } catch (Exception e) {
            logger.info("socketChannel::read fail, close socket.");
            close(socketChannel);
            return;
        }

        selectionKey.interestOps(SelectionKey.OP_WRITE);
    }

    public String execCommand(String commandLine) throws CloneNotSupportedException {
        Command command = new CommandFactory().getCommand(commandLine);
        if(command == null) {
            logger.error("command line \"{}\" is invalid.", commandLine);
            return "[Invalid Method]";
        }

        return command.exec().wrap();
    }

    public void close(SocketChannel socketChannel) {
        try {
            socketChannel.close();
        } catch (Exception e) {
            logger.error("fail to close socket, address is {}:{}.", hostName, port);
            e.printStackTrace();
        }
    }
}
