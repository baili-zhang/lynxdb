package zbl.moonlight.server.io;

import lombok.Data;
import zbl.moonlight.server.protocol.MdtpRequest;
import zbl.moonlight.server.protocol.MdtpResponse;

import java.io.IOException;
import java.nio.channels.*;

@Data
public class IoEvent {
    private boolean finished;
    /**
     * 下一次注册的事件
     */
    private int operator;

    /**
     * 处理连接事件时使用
     */
    private SocketChannel socketChannel;

    /**
     * Mdtp 请求对象
     */
    private MdtpRequest mdtpRequest;

    /**
     * Mdtp 响应对象
     */
    private MdtpResponse mdtpResponse;

    public IoEvent(int operator) {
        this.operator = operator;
        this.finished = false;
    }

    public void handle(Selector selector, SelectionKey selectionKey) throws IOException {
        if(selectionKey.isAcceptable()) {
            IoEvent ioEvent = new IoEvent(SelectionKey.OP_WRITE);
            ioEvent.setMdtpRequest(new MdtpRequest());
            socketChannel.register(selector, SelectionKey.OP_READ, ioEvent);
            return;
        }

        if(!finished) {
            return;
        }

        selectionKey.interestOps(operator);
        operator = operator == SelectionKey.OP_WRITE ? SelectionKey.OP_READ : SelectionKey.OP_WRITE;
        finished = false;
    }
}
