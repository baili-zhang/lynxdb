package zbl.moonlight.server.eventbus.subscriber;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.eventbus.Subscriber;
import zbl.moonlight.server.log.Log;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryLogSubscriber implements Subscriber {
    private final Logger logger = LogManager.getLogger("BinaryLogSubscriber");
    private final Log log = new Log();

    public BinaryLogSubscriber() throws IOException {
    }

    @Override
    public void handle(MdtpRequest request) {
        byte method = request.getMethod();
        if(method != MdtpMethod.SET && method != MdtpMethod.DELETE) {
            return;
        }

        System.out.println(MdtpMethod.getMethodName(method));

        ByteBuffer header = request.getHeader();
        ByteBuffer key = request.getKey();
        DynamicByteBuffer value = request.getValue();

        header.rewind();
        key.rewind();
        value.rewind();

        try {
            log.append(request.getHeader());
            log.append(request.getKey());
            for(ByteBuffer buffer : value.getBufferList()) {
                log.append(buffer);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(value != null) {
            value.rewind();
        }
        logger.info("write mdtp request to binary log.");
    }
}
