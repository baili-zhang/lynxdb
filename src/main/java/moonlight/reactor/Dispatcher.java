package moonlight.reactor;

import java.io.IOException;
import java.nio.channels.*;
import java.util.*;

public class Dispatcher {
    private volatile Selector selector;

    public Dispatcher(Selector selector) {
        this.selector = selector;
    }

    public void handleEvents() throws IOException {
        synchronized (EventHandler.class) {
            selector.select();
            Set<SelectionKey> selectionKeys = selector.selectedKeys();

            Iterator<SelectionKey> iterator = selectionKeys.iterator();

            while (iterator.hasNext()) {
                SelectionKey selectionKey = iterator.next();
                Runnable handler = (Runnable) selectionKey.attachment();

                if(handler != null) {
                    handler.run();
                }

                iterator.remove();
            }
        }
    }
}
