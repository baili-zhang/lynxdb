package zbl.moonlight.core.raft.state;

import zbl.moonlight.core.raft.request.Entry;

@FunctionalInterface
public interface Appliable {
    void apply(Entry[] entries);
}
