package zbl.moonlight.server.engine;

import zbl.moonlight.server.protocol.Mdtp;
import zbl.moonlight.server.protocol.MdtpMethod;

public abstract class Engine {
    public final void exec(Mdtp mdtp) {
        switch (mdtp.getMethod()) {
            case MdtpMethod.SET:
                set(mdtp);
                break;
            case MdtpMethod.GET:
                get(mdtp);
                break;
            case MdtpMethod.UPDATE:
                update(mdtp);
                break;
            case MdtpMethod.DELETE:
                delete(mdtp);
        }
    }

    protected abstract void set(Mdtp mdtp);
    protected abstract void get(Mdtp mdtp);
    protected abstract void update(Mdtp mdtp);
    protected abstract void delete(Mdtp mdtp);
}
