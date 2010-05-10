package com.g414.inno.db;

import java.util.List;

import com.g414.inno.db.Transaction.State;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Cursor {
    private final PointerByReference crsr;

    public Cursor(PointerByReference crsr) {
        this.crsr = crsr;
    }

    public PointerByReference getCrsr() {
        return crsr;
    }

    public void first() {
        Util.assertSuccess(InnoDB.ib_cursor_first(crsr.getValue()));
    }

    public void lock(Lock mode) {
        Util.assertSuccess(InnoDB.ib_cursor_lock(crsr.getValue(), mode
                .getCode()));
    }

    public void insertRows(List<TupleBuilder> tuples) {
        Pointer tupl = InnoDB.ib_clust_read_tuple_create(crsr.getValue());
        try {
            for (TupleBuilder tuple : tuples) {
                tupl = insertRow(tupl, tuple);
            }
        } finally {
            InnoDB.ib_tuple_delete(tupl);
        }
    }

    public void insertRows(TupleBuilder... tuples) {
        Pointer tupl = InnoDB.ib_clust_read_tuple_create(crsr.getValue());
        try {
            for (TupleBuilder tuple : tuples) {
                tupl = insertRow(tupl, tuple);
            }
        } finally {
            InnoDB.ib_tuple_delete(tupl);
        }
    }

    private Pointer insertRow(Pointer tupl, TupleBuilder tuple) {
        for (ColumnValue val : tuple.getValues()) {
            if (val.getValue() instanceof String) {
                Util.assertSuccess(InnoDB.ib_col_set_value(tupl, val
                        .getColIndex(), Types
                        .getString((String) val.getValue()), val.getLength()));
            } else if (val.getValue() instanceof Integer) {
                switch (val.getLength()) {
                case 4:
                    Util.assertSuccess(InnoDB.ib_tuple_write_u32(tupl, val
                            .getColIndex(), ((Integer) val.getValue())
                            .intValue()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "int type not fully supported");
                }
            } else if (val.getValue() instanceof Long) {
                switch (val.getLength()) {
                case 8:
                    Util
                            .assertSuccess(InnoDB.ib_tuple_write_u64(tupl, val
                                    .getColIndex(), ((Long) val.getValue())
                                    .intValue()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "long type not fully supported");
                }
            } else {
                throw new IllegalArgumentException("unsupported type: "
                        + val.getValue().getClass() + " : "
                        + val.getValue().toString());
            }
        }
        Util.assertSuccess(InnoDB.ib_cursor_insert_row(crsr.getValue(), tupl));
        tupl = InnoDB.ib_tuple_clear(tupl);
        return tupl;
    }

    public void close() {
        Util.assertSuccess(InnoDB.ib_cursor_close(crsr.getValue()));
    }

    public enum Lock {
        INTENTION_SHARED(InnoDB.ib_lck_mode_t.IB_LOCK_IS), INTENTION_EXCLUSIVE(
                InnoDB.ib_lck_mode_t.IB_LOCK_IX), LOCK_SHARED(
                InnoDB.ib_lck_mode_t.IB_LOCK_S), LOCK_EXCLUSIVE(
                InnoDB.ib_lck_mode_t.IB_LOCK_X), NOT_USED(
                InnoDB.ib_lck_mode_t.IB_LOCK_NOT_USED), NONE(
                InnoDB.ib_lck_mode_t.IB_LOCK_NONE);

        private final int code;

        private Lock(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static State fromCode(int code) {
            return State.values()[code];
        }
    }
}
