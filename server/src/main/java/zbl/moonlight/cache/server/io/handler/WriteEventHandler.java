package zbl.moonlight.cache.server.io.handler;

import zbl.moonlight.cache.server.io.EventHandler;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WriteEventHandler extends EventHandler {
    private SelectionKey selectionKey;

    public WriteEventHandler(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
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
            System.out.println("Fail to close socket !");
            exception.printStackTrace();
        }
    }
}
