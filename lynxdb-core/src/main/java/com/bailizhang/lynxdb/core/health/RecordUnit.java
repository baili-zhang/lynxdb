package com.bailizhang.lynxdb.core.health;

public enum RecordUnit {
    MILLIS("ms", (byte)0x01),
    NANOS("ns", (byte)0x02),
    TIMES("times", (byte) 0x03);

    final byte value;
    final String name;

    public static RecordUnit find(byte val) {
        RecordUnit[] units = values();

        for (RecordUnit unit : units) {
            if(unit.value == val) {
                return unit;
            }
        }

        throw new RuntimeException();
    }

    RecordUnit(String unitName, byte unitValue) {
        name = unitName;
        value = unitValue;
    }

    public byte value() {
        return value;
    }

    public String unitName() {
        return name;
    }
}
