package rcache.reactor;

public abstract class EventHandler {
    private Handle handle;
    public abstract void handleEvent();

    public Handle getHandle() {
        return handle;
    }

    public void setHandle(Handle handle) {
        this.handle = handle;
    }
}
