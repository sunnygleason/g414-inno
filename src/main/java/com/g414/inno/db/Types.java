package com.g414.inno.db;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class Types {
    public static Pointer getString(String string) {
        if (string.length() > 1023) {
            throw new IllegalArgumentException("string > 1023 characters");
        }

        Memory m = new Memory(1024);
        ByteBuffer buf = m.getByteBuffer(0, m.getSize()).order(
                ByteOrder.nativeOrder());
        buf.put(string.getBytes()).put((byte) 0).flip();

        return Native.getDirectBufferPointer(buf);
    }
}
