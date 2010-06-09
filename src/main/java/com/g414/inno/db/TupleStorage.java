package com.g414.inno.db;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;

import com.g414.inno.db.impl.Util;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;

public class TupleStorage {
    public static Number loadInteger(Tuple tupl, int i, ColumnDef def) {
        int length = def.getLength();
        boolean signed = !def.getAttrs().contains(ColumnAttribute.UNSIGNED);

        ByteBuffer buf = ByteBuffer.allocateDirect(length);

        InnoDB.ib_col_meta_t meta = new InnoDB.ib_col_meta_t();
        if (InnoDB.ib_col_get_meta(tupl.tupl, i, meta) == InnoDB.IB_SQL_NULL) {
            return null;
        }

        switch (length) {
        case 1:
            Util.assertSuccess(InnoDB.ib_tuple_read_u8(tupl.tupl, i, buf));
            break;

        case 2:
            ShortBuffer sbuf = buf.asShortBuffer();
            Util.assertSuccess(InnoDB.ib_tuple_read_u16(tupl.tupl, i, sbuf));
            break;

        case 4:
            IntBuffer ibuf = buf.asIntBuffer();
            Util.assertSuccess(InnoDB.ib_tuple_read_u32(tupl.tupl, i, ibuf));
            break;

        case 8:
            LongBuffer lbuf = buf.asLongBuffer();
            Util.assertSuccess(InnoDB.ib_tuple_read_u64(tupl.tupl, i, lbuf));
            break;

        default:
            throw new IllegalArgumentException("Invalid length: " + length);
        }

        byte[] theBytes = new byte[length];
        int k = 0;
        for (int j = length - 1; j >= 0; j--) {
            theBytes[k] = buf.get(j);
            k += 1;
        }
        return signed ? new BigInteger(theBytes) : new BigInteger(1, theBytes);
    }

    public static void storeInteger(Tuple tupl, ColumnDef colDef, int i,
            Number numVal) {
        switch (colDef.getLength()) {
        case 1:
            Util.assertSuccess(InnoDB.ib_tuple_write_u8(tupl.tupl, i, numVal
                    .byteValue()));
            break;
        case 2:
            Util.assertSuccess(InnoDB.ib_tuple_write_u16(tupl.tupl, i, numVal
                    .shortValue()));
            break;
        case 4:
            Util.assertSuccess(InnoDB.ib_tuple_write_u32(tupl.tupl, i, numVal
                    .intValue()));
            break;
        case 8:
            Util.assertSuccess(InnoDB.ib_tuple_write_u64(tupl.tupl, i, numVal
                    .longValue()));
            break;
        default:
            throw new IllegalArgumentException(
                    "integer type not supported for length: "
                            + colDef.getLength());
        }
    }

    public static void storeBytes(Tuple tupl, int i, byte[] val) {
        Util.assertSuccess(InnoDB.ib_col_set_value(tupl.tupl, i, TupleStorage
                .getDirectMemoryBytes(val), val.length));
    }

    public static byte[] loadBytes(Tuple tupl, int index) {
        int len = InnoDB.ib_col_get_len(tupl.tupl, index);
        if (len == InnoDB.IB_SQL_NULL) {
            return null;
        }

        return InnoDB.ib_col_get_value(tupl.tupl, index).getByteArray(0, len);
    }

    public static void storeString(Tuple tupl, int i, String stringVal) {
        try {
            byte[] stringBytes = stringVal.getBytes("UTF-8");
            Pointer stringPointer = TupleStorage.getDirectMemoryString(stringBytes);
            Util.assertSuccess(InnoDB.ib_col_set_value(tupl.tupl, i,
                    stringPointer, stringBytes.length + 1));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Java doesn't recognize UTF-8?!");
        }
    }

    public static String loadString(Tuple tupl, int index) {
        int len = InnoDB.ib_col_get_len(tupl.tupl, index);
        if (len == InnoDB.IB_SQL_NULL) {
            return null;
        }

        return InnoDB.ib_col_get_value(tupl.tupl, index).getString(0);
    }

    private static Pointer getDirectMemoryBytes(byte[] value) {
        if (value == null || value.length == 0) {
            return Pointer.NULL;
        }

        int len = value.length;
        Memory m = new Memory(len);
        m.getByteBuffer(0, len).put(value);

        return Native.getDirectBufferPointer(m.getByteBuffer(0, len));
    }

    public static Pointer getDirectMemoryString(byte[] stringBytes) {
        if (stringBytes == null || stringBytes.length == 0) {
            return Pointer.NULL;
        }

        int length = stringBytes.length + 1;
        Memory m = new Memory(length);
        ByteBuffer buf = m.getByteBuffer(0, length).order(
                ByteOrder.nativeOrder());

        buf.put(stringBytes).put((byte) 0).flip();
        return Native.getDirectBufferPointer(buf);
    }
}
