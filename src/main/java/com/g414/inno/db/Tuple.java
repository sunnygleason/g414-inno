package com.g414.inno.db;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;
import java.nio.ShortBuffer;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.g414.inno.db.impl.Util;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;

public class Tuple {
    protected final Pointer tupl;
    protected final TableDef table;

    public Tuple(Pointer tupl, TableDef table) {
        this.tupl = tupl;
        this.table = table;
    }

    public List<Object> values() {
        List<ColumnDef> colDefs = table.getColDefs();
        final int len = colDefs.size();

        List<Object> values = new ArrayList<Object>(len);

        for (int i = 0; i < len; i++) {
            ColumnDef def = colDefs.get(i);
            switch (def.getType()) {
            case BINARY:
            case VARBINARY:
            case BLOB:
                values.add(getBytes(i));
                break;
            case CHAR:
            case CHAR_ANYCHARSET:
            case VARCHAR:
            case VARCHAR_ANYCHARSET:
                values.add(getString(i));
                break;
            case INT:
                values.add(getInteger(i, def.getLength(), !def.getAttrs()
                        .contains(ColumnAttribute.UNSIGNED)));
                break;
            default:
                throw new IllegalArgumentException("unsupported datatype: "
                        + def.getType());
            }
        }

        return values;
    }

    public Map<String, Object> valueMap() {
        List<ColumnDef> colDefs = table.getColDefs();
        final int len = colDefs.size();

        Map<String, Object> values = new LinkedHashMap<String, Object>(len);

        for (int i = 0; i < len; i++) {
            ColumnDef def = colDefs.get(i);
            switch (def.getType()) {
            case BINARY:
            case VARBINARY:
            case BLOB:
                values.put(def.getName(), getBytes(i));
                break;
            case CHAR:
            case CHAR_ANYCHARSET:
            case VARCHAR:
            case VARCHAR_ANYCHARSET:
                values.put(def.getName(), getString(i));
                break;
            case INT:
                values.put(def.getName(), getInteger(i, def.getLength(), !def
                        .getAttrs().contains(ColumnAttribute.UNSIGNED)));
                break;
            default:
                throw new IllegalArgumentException("unsupported datatype: "
                        + def.getType());
            }
        }

        return values;
    }

    public byte[] getBytes(int index) {
        int len = InnoDB.ib_col_get_len(tupl, index);
        if (len == InnoDB.IB_SQL_NULL) {
            return null;
        }

        return InnoDB.ib_col_get_value(tupl, index).getByteArray(0, len);
    }

    public String getString(int index) {
        int len = InnoDB.ib_col_get_len(tupl, index);
        if (len == InnoDB.IB_SQL_NULL) {
            return null;
        }

        return InnoDB.ib_col_get_value(tupl, index).getString(0);
    }

    public Number getInteger(int index, Integer length, boolean signed) {
        ByteBuffer buf = ByteBuffer.allocateDirect(length);

        InnoDB.ib_col_meta_t meta = new InnoDB.ib_col_meta_t();
        if (InnoDB.ib_col_get_meta(tupl, index, meta) == InnoDB.IB_SQL_NULL) {
            return null;
        }

        switch (length) {
        case 1:
            Util.assertSuccess(InnoDB.ib_tuple_read_u8(tupl, index, buf));
            break;

        case 2:
            ShortBuffer sbuf = buf.asShortBuffer();
            Util.assertSuccess(InnoDB.ib_tuple_read_u16(tupl, index, sbuf));
            break;

        case 4:
            IntBuffer ibuf = buf.asIntBuffer();
            Util.assertSuccess(InnoDB.ib_tuple_read_u32(tupl, index, ibuf));
            break;

        case 8:
            LongBuffer lbuf = buf.asLongBuffer();
            Util.assertSuccess(InnoDB.ib_tuple_read_u64(tupl, index, lbuf));
            break;

        default:
            throw new IllegalArgumentException("Invalid length: " + length);
        }

        byte[] theBytes = new byte[length];
        int i = 0;
        for (int j = length - 1; j >= 0; j--) {
            theBytes[i] = buf.get(j);
            i += 1;
        }
        return signed ? new BigInteger(theBytes) : new BigInteger(1, theBytes);
    }

    public void clear() {
        InnoDB.ib_tuple_clear(tupl);
    }

    public void delete() {
        InnoDB.ib_tuple_delete(tupl);
    }
}
