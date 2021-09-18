package moonlight.reactor;

public enum EventType {
    ACCEPT_EVENT (1 << 0),
    READ_EVENT (1 << 1),
    WRITE_EVENT (1 << 2),
    TIMEOUT_EVENT (1 << 3),
    SIGNAL_EVENT(1 << 4),
    CLOSE_EVENT(1 << 5);

    private int value;

    EventType(int  value) {
        this.value = value;
    }
}
