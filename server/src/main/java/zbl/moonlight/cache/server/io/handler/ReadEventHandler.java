package zbl.moonlight.cache.server.io.handler;

import zbl.moonlight.cache.server.command.Command;
import zbl.moonlight.cache.server.command.CommandFactory;
import zbl.moonlight.cache.server.io.EventHandler;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

public class ReadEventHandler extends EventHandler {
    private SelectionKey selectionKey;

    public ReadEventHandler(SelectionKey selectionKey) {
        this.selectionKey = selectionKey;
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
            System.out.println("[READ] Command line is: " + commandLine);
            String result = execCommand(commandLine);
            byteBuffer.clear();
            byteBuffer.put(result.getBytes());
        } catch (Exception e) {
            System.out.println("socketChannel.read fail, close socket !");
            close(socketChannel);
            e.printStackTrace();
            return;
        }

        selectionKey.interestOps(SelectionKey.OP_WRITE);
    }

    public String execCommand(String commandLine) {
        Command command = new CommandFactory().getCommand(commandLine);
        if(command == null) {
            System.out.println("command line: " + commandLine);
            return "[Invalid Method]";
        }

        return command.exec().wrap();
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
