package com.g414.inno.db.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class Types {
    public static Pointer getBytes(byte[] value) {
        Memory m = new Memory(value.length);
        ByteBuffer buf = m.getByteBuffer(0, m.getSize()).order(
                ByteOrder.nativeOrder());
        buf.put(value).flip();

        return Native.getDirectBufferPointer(buf);
    }

    public static Pointer getString(String string) {
        Memory m = new Memory(string.length() + 1);
        ByteBuffer buf = m.getByteBuffer(0, m.getSize()).order(
                ByteOrder.nativeOrder());
        try {
            buf.put(string.getBytes("UTF-8")).put((byte) 0).flip();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Java doesn't recognize UTF-8?!");
        }

        return Native.getDirectBufferPointer(buf);
    }
}
