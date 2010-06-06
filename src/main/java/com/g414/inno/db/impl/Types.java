package com.g414.inno.db.impl;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class Types {
    public static Pointer getBytes(byte[] value) {
        if (value == null) {
            return Memory.NULL;
        }

        Memory m = new Memory(value.length);
        m.write(0, value, 0, value.length);
        // ByteBuffer buf = m.getByteBuffer(0, m.getSize()).order(
        // ByteOrder.nativeOrder());
        // buf.put((byte) 0xFF).put(value).put((byte) 0xFF).flip();

        return m;
    }

    public static Pointer getString(String string) {
        if (string == null) {
            return Memory.NULL;
        }

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
