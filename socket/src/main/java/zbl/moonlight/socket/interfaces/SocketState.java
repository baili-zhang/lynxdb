package zbl.moonlight.socket.interfaces;

public interface SocketState {
    byte EMPTY_FLAG = (byte) 0;
    /**
     * 保持连接标志位
     */
    byte STAY_CONNECTED_FLAG = (byte) 1;
    /**
     * 广播标志位
     */
    byte BROADCAST_FLAG = (byte) 1 << 1;

    static boolean isStayConnected(byte status) {
        return (status & STAY_CONNECTED_FLAG) != 0;
    }

    static boolean isBroadcast(byte status) {
        return (status & BROADCAST_FLAG) != 0;
    }
}
