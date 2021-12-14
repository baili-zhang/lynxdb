package zbl.moonlight.server.io.handler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.io.EventHandler;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WriteEventHandler extends EventHandler {
    private static final Logger logger = LogManager.getLogger("WriteEventHandler");

    public WriteEventHandler(SelectionKey selectionKey) {
        super(selectionKey);
    }

    @Override
    public void run() {
        ByteBuffer byteBuffer = (ByteBuffer) selectionKey.attachment();
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        byteBuffer.flip();

        /* moonlight.client ask to disconnect or not */
        boolean isConnectionOver = new String(byteBuffer.array()).trim().equals("[Close Connection]");

        try {
            socketChannel.write(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (isConnectionOver) {
            close(socketChannel);
            return;
        }

        byteBuffer.clear();
        selectionKey.interestOps(SelectionKey.OP_READ);
    }

    public void close(SocketChannel socketChannel) {
        try {
            socketChannel.close();
        } catch (Exception exception) {
            logger.error("fail to close socket, address is {}:{}.", hostName, port);
            exception.printStackTrace();
        }
    }
}
