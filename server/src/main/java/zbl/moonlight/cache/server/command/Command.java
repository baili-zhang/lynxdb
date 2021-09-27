package zbl.moonlight.cache.server.command;

import lombok.Data;

@Data
public abstract class Command {
    protected String key;
    protected String value;
    protected boolean isKeyExisted;

    public Command(String key, String value) {
        this.key = key;
        this.value = value;
    }

    public abstract Command exec();
    public abstract String wrap();
}
