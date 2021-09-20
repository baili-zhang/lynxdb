package moonlight.reactor;

import moonlight.command.Command;
import moonlight.command.CommandFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;

public class EventHandler implements Runnable {
    private SelectionKey selectionKey;
    private SocketChannel socketChannel;
    private Selector selector;
    private volatile int state = PROCESSED;
    private ByteBuffer byteBuffer = ByteBuffer.allocate(1024);

    static final int PROCESSING = 1 << 0;
    static final int PROCESSED = 1 << 1;

    public EventHandler(SocketChannel socketChannel, Selector selector) throws IOException {
        this.socketChannel = socketChannel;
        this.selector = selector;
        socketChannel.configureBlocking(false);

        this.selectionKey = socketChannel.register(selector, SelectionKey.OP_READ, this);
        selector.wakeup();
    }

    @Override
    public void run() {
        if(state == PROCESSED) {
            WorkerPool.getInstance().execute(new Process(selectionKey));
        }
    }

    class Process implements Runnable {
        private SelectionKey selectionKey;

        public Process(SelectionKey selectionKey) {
            this.selectionKey = selectionKey;
            state = PROCESSING;
        }

        @Override
        public void run() {
            try {
                if(socketChannel.socket().isClosed()) {
                    return;
                }
                if(selectionKey.isReadable()) {
                    read();
                } else if (selectionKey.isWritable()) {
                    write();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public void read() {
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
        }

        selectionKey.interestOps(SelectionKey.OP_WRITE);
        state = PROCESSED;
        selector.wakeup();
    }

    public void write() {
        byteBuffer.flip();

        /* client ask to disconnect or not */
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
        state = PROCESSED;
        selector.wakeup();
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
            socketChannel.close();
        } catch (Exception exception) {
            System.out.println("Fail to close socket !");
            exception.printStackTrace();
        }
    }
}
