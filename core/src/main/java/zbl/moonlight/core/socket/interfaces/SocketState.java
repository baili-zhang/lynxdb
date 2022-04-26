package zbl.moonlight.core.socket.interfaces;

public interface SocketState {
    /** 保持socket连接 */
    byte STAY_CONNECTED = (byte) 0x01;
    byte DISCONNECTED = (byte) 0x02;
}
