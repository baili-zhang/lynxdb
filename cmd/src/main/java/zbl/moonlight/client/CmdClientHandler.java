package zbl.moonlight.client;

import lombok.Setter;
import zbl.moonlight.client.printer.Printer;
import zbl.moonlight.core.utils.BufferUtils;
import zbl.moonlight.server.engine.result.Result;
import zbl.moonlight.socket.interfaces.SocketClientHandler;
import zbl.moonlight.socket.response.SocketResponse;

import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

public class CmdClientHandler implements SocketClientHandler {
    private final CyclicBarrier barrier;
    @Setter
    private MoonlightCmd client;

    CmdClientHandler(CyclicBarrier barrier) {
        this.barrier = barrier;
    }

    @Override
    public void handleConnected(SelectionKey selectionKey) throws Exception {
        Printer.printConnected(((SocketChannel)selectionKey.channel()).getRemoteAddress());
        barrier.await();
    }

    @Override
    public void handleResponse(SocketResponse response) throws BrokenBarrierException, InterruptedException {
        int serial = response.serial();
        byte[] data = response.data();

        ByteBuffer buffer = ByteBuffer.wrap(data);
        byte code = buffer.get();

        switch (code) {
            case Result.SUCCESS -> Printer.printOK();
            case Result.SUCCESS_SHOW_COLUMN -> handleShowColumn(buffer);
            case Result.SUCCESS_SHOW_TABLE -> handleShowTable(buffer);
            case Result.Error.INVALID_ARGUMENT -> {
                String message = BufferUtils.getRemainingString(buffer);
                Printer.printError(message);
            }

            default -> Printer.printError("Unknown Response Status Code");
        }

        barrier.await();
    }

    private void handleShowTable(ByteBuffer buffer) {
        int columnSize = buffer.getInt();
        List<List<String>> table = new ArrayList<>();

        while(!BufferUtils.isOver(buffer)) {
            List<String> row = new ArrayList<>();
            for(int i = 0; i < columnSize; i ++) {
                row.add(BufferUtils.getString(buffer));
            }
            table.add(row);
        }

        Printer.printTable(table);
    }

    private void handleShowColumn(ByteBuffer buffer) {
        List<String> total = BufferUtils.toStringList(buffer);
        List<List<String>> table = total.stream().map(List::of).toList();
        Printer.printTable(table);
    }

    @Override
    public void handleConnectFailure(SelectionKey selectionKey) throws Exception {
        String message = String.format("Connect to [%s] failure", ((SocketChannel)selectionKey.channel()).getRemoteAddress());
        Printer.printError(message);
        /* 清空客户端的当前节点 */
        client.setCurrent(null);
        barrier.await();
    }
}
