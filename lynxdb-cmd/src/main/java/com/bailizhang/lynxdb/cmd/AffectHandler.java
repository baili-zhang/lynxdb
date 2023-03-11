package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.message.MessageHandler;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.ldtp.affect.AffectValue;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class AffectHandler implements MessageHandler {

    @Override
    public void doHandle(MessageKey messageKey, ByteBuffer buffer) {
        HashMap<String, byte[]> multiColumns = AffectValue.valuesFrom(buffer);
        AffectValue affectValue = new AffectValue(messageKey, multiColumns);

        printAffectValue(affectValue);

        Printer.printPrompt(null);
    }

    private void printAffectValue(AffectValue affectValue) {
        MessageKey messageKey = affectValue.messageKey();
        HashMap<String, byte[]> multiColumns = affectValue.multiColumns();

        String template = "\nAffect dbKey: %s, columnFamily: %s";
        String message = String.format(
                template,
                G.I.toString(messageKey.key()),
                messageKey.columnFamily()
        );

        Printer.printRawMessage(message);
        Printer.printDbValues(multiColumns);
    }
}
