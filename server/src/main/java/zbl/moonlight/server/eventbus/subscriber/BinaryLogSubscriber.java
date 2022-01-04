package zbl.moonlight.server.eventbus.subscriber;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import zbl.moonlight.server.engine.buffer.DynamicByteBuffer;
import zbl.moonlight.server.eventbus.Subscriber;
import zbl.moonlight.server.log.BinaryLog;
import zbl.moonlight.server.protocol.MdtpMethod;
import zbl.moonlight.server.protocol.MdtpRequest;

import java.io.IOException;
import java.nio.ByteBuffer;

public class BinaryLogSubscriber implements Subscriber {
    private final Logger logger = LogManager.getLogger("BinaryLogSubscriber");
    private final BinaryLog binaryLog;

    public BinaryLogSubscriber(BinaryLog binaryLog) {
        this.binaryLog = binaryLog;
    }

    @Override
    public void handle(MdtpRequest request) {
        byte method = request.getMethod();
        if(method != MdtpMethod.SET && method != MdtpMethod.DELETE) {
            return;
        }

        ByteBuffer header = request.getHeader();
        ByteBuffer key = request.getKey();
        DynamicByteBuffer value = request.getValue();

        header.rewind();
        key.rewind();

        logger.info("write to binary log, request is: " + request);
        if(value != null) {
            value.rewind();
        }

        try {
            binaryLog.append(request.getHeader());
            binaryLog.append(request.getKey());
            if(value != null) {
                for(ByteBuffer buffer : value.getBufferList()) {
                    binaryLog.append(buffer);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        if(value != null) {
            value.rewind();
        }
    }
}
