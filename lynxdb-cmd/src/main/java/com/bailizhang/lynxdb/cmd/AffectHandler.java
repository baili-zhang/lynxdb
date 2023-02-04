package com.bailizhang.lynxdb.cmd;

import com.bailizhang.lynxdb.client.LynxDbClient;
import com.bailizhang.lynxdb.client.message.MessageHandler;
import com.bailizhang.lynxdb.cmd.printer.Printer;
import com.bailizhang.lynxdb.core.common.G;
import com.bailizhang.lynxdb.ldtp.affect.AffectValue;
import com.bailizhang.lynxdb.ldtp.message.MessageKey;
import com.bailizhang.lynxdb.lsmtree.common.DbValue;

import java.nio.ByteBuffer;
import java.util.List;

public class AffectHandler implements MessageHandler {
    private final LynxDbClient client;

    public AffectHandler(LynxDbClient lynxDbClient) {
        client = lynxDbClient;
    }

    @Override
    public void doHandle(MessageKey messageKey, ByteBuffer buffer) {
        List<DbValue> dbValues = AffectValue.valuesFrom(buffer);
        AffectValue affectValue = new AffectValue(messageKey, dbValues);

        printAffectValue(affectValue);

        Printer.printPrompt(client.selectionKey());
    }

    private void printAffectValue(AffectValue affectValue) {
        MessageKey messageKey = affectValue.messageKey();
        List<DbValue> dbValues = affectValue.dbValues();

        String template = "\nAffect key: %s, columnFamily: %s";
        String message = String.format(
                template,
                G.I.toString(messageKey.key()),
                G.I.toString(messageKey.columnFamily())
        );

        Printer.printRawMessage(message);
        Printer.printDbValues(dbValues);
    }
}
