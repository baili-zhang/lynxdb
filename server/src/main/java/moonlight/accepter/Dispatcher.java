package moonlight.accepter;

import java.io.IOException;
import java.nio.channels.*;
import java.util.*;

public class Dispatcher {
    private Selector selector;

    public Dispatcher(Selector selector) {
        this.selector = selector;
    }

    public void handleEvents() throws IOException {
        Set<SelectionKey> selectionKeys;
        Iterator<SelectionKey> iterator;

        selector.select();
        selectionKeys = selector.selectedKeys();
        iterator = selectionKeys.iterator();

        while (iterator.hasNext()) {
            SelectionKey selectionKey = iterator.next();
            Runnable handler = (Runnable) selectionKey.attachment();

            if (handler != null) {
                handler.run();
            }
            iterator.remove();
        }
    }
}
