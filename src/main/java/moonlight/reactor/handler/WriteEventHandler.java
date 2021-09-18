package moonlight.reactor.handler;

import moonlight.reactor.Dispatcher;
import moonlight.reactor.EventHandler;
import moonlight.reactor.EventType;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class WriteEventHandler extends EventHandler {

    public WriteEventHandler(SelectionKey selectionKey) {
        super(selectionKey);
    }

    @Override
    public void run() {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer byteBuffer = (ByteBuffer) selectionKey.attachment();
        byteBuffer.flip();

        /* client ask to disconnect or not */
        boolean isConnectionOver = new String(byteBuffer.array()).trim().equals("[Close Connection]");

        try {
            socketChannel.write(byteBuffer);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Dispatcher dispatcher = Dispatcher.getInstance();

        if(isConnectionOver) {
            try {
                socketChannel.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

            dispatcher.removeHandler(this, EventType.WRITE_EVENT);
            System.out.println("Close a connection !");

            return;
        }

        byteBuffer.clear();

        /**
         * remove WRITE event handler and register READ event handler
         */
        try {
            dispatcher.registerHandler(new ReadEventHandler(selectionKey), EventType.READ_EVENT);
        } catch (Exception e) {
            e.printStackTrace();
        }

        dispatcher.removeHandlingEvent(selectionKey);
    }
}
