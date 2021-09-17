package rcache.reactor.handler;

import rcache.command.Command;
import rcache.command.CommandFactory;
import rcache.reactor.Dispatcher;
import rcache.reactor.EventHandler;
import rcache.reactor.EventType;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadEventHandler extends EventHandler {
    public ReadEventHandler(SelectionKey selectionKey) {
        super(selectionKey);
    }

    @Override
    public void run() {
        SocketChannel socketChannel = (SocketChannel) selectionKey.channel();
        ByteBuffer byteBuffer = (ByteBuffer) selectionKey.attachment();
        try {
            int n = socketChannel.read(byteBuffer);
            byteBuffer.flip();

            /* read command line from byteBuffer */
            String commandLine = new String(byteBuffer.array(), 0, n).trim();
            String result = execCommand(commandLine);
            byteBuffer.clear();

            /* write result to byteBuffer */
            byteBuffer.put(result.getBytes());

        } catch (Exception e) {
            System.out.println("socketChannel.read fail, close socket !");
            close(socketChannel);
            e.printStackTrace();
        }

        try {
            Dispatcher dispatcher = Dispatcher.getInstance();
            dispatcher.registerHandler(new WriteEventHandler(selectionKey), EventType.WRITE_EVENT);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public String execCommand(String commandLine) {
        Command command = new CommandFactory().getCommand(commandLine);
        if(command == null) {
            return "[Invalid Method]";
        }

        return command.exec().wrap();
    }

    public void close(SocketChannel socketChannel) {
        try {
            Dispatcher dispatcher = Dispatcher.getInstance();
            dispatcher.removeHandler(this, EventType.READ_EVENT);
            socketChannel.close();
        } catch (Exception exception) {
            System.out.println("Fail to close socket !");
            exception.printStackTrace();
        }
    }
}
