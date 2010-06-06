package com.g414.inno.db;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

import com.g414.inno.db.impl.Types;
import com.g414.inno.db.impl.Util;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Cursor {
    private final PointerByReference crsr;
    private final TableDef table;

    private volatile int err = InnoDB.db_err.DB_SUCCESS;

    public Cursor(PointerByReference crsr, TableDef table) {
        this.crsr = crsr;
        this.table = table;
    }

    public PointerByReference getCrsr() {
        return crsr;
    }

    public Tuple createReadTuple() {
        return new Tuple(InnoDB.ib_clust_read_tuple_create(crsr.getValue()),
                table);
    }

    public Tuple createSearchTuple(TupleBuilder tuple) {
        Tuple searchTuple = new Tuple(InnoDB.ib_clust_search_tuple_create(crsr
                .getValue()), table);

        List<Object> values = tuple.getValues();

        for (int i = 0; i < tuple.getSize(); i++) {
            Object value = values.get(i);
            ColumnDef colDef = table.getColDefs().get(i);
            // System.out.println(colDef.getName() + " " + value);
            setValue(searchTuple, colDef, i, value);
        }

        return searchTuple;
    }

    public SearchResultCode find(Tuple tupl, SearchMode searchMode) {
        IntBuffer result = ByteBuffer.allocateDirect(4).asIntBuffer();
        int err = InnoDB.ib_cursor_moveto(crsr.getValue(), tupl.tupl,
                searchMode.getCode(), result);
        assertCursorState(err);

        return SearchResultCode.fromCode(result.get());
    }

    public void readRow(Tuple tupl) {
        int err = InnoDB.ib_cursor_read_row(crsr.getValue(), tupl.tupl);
        assertCursorState(err);
    }

    public void deleteRow() {
        int err = InnoDB.ib_cursor_delete_row(crsr.getValue());
        assertCursorState(err);
    }

    public void first() {
        Util.assertSuccess(InnoDB.ib_cursor_first(crsr.getValue()));
    }

    public void last() {
        Util.assertSuccess(InnoDB.ib_cursor_last(crsr.getValue()));
    }

    public boolean hasNext() {
        return err == InnoDB.db_err.DB_SUCCESS;
    }

    public void prev() {
        err = InnoDB.ib_cursor_prev(crsr.getValue());
        assertCursorState(err);
    }

    public void next() {
        err = InnoDB.ib_cursor_next(crsr.getValue());
        assertCursorState(err);
    }

    public void lock(Lock mode) {
        Util.assertSuccess(InnoDB.ib_cursor_lock(crsr.getValue(), mode
                .getCode()));
    }

    public void insertRows(List<TupleBuilder> tuples) {
        Pointer tupl = InnoDB.ib_clust_read_tuple_create(crsr.getValue());
        Tuple tuple = new Tuple(tupl, table);

        try {
            for (TupleBuilder tupleValues : tuples) {
                insertRow(tuple, tupleValues);
            }
        } finally {
            InnoDB.ib_tuple_delete(tupl);
        }
    }

    public void insertRows(TupleBuilder... tuples) {
        Pointer tupl = InnoDB.ib_clust_read_tuple_create(crsr.getValue());
        Tuple tuple = new Tuple(tupl, table);

        try {
            for (TupleBuilder tupleValues : tuples) {
                insertRow(tuple, tupleValues);
            }
        } finally {
            InnoDB.ib_tuple_delete(tupl);
        }
    }

    public void insertRow(Tuple tupl, TupleBuilder tuple) {
        List<Object> values = tuple.getValues();
        TableDef def = tuple.getDef();
        List<ColumnDef> colDefs = def.getColDefs();

        int len = values.size();

        for (int i = 0; i < len; i++) {
            Object val = values.get(i);
            ColumnDef colDef = colDefs.get(i);

            setValue(tupl, colDef, i, val);
        }

        Util.assertSuccess(InnoDB.ib_cursor_insert_row(crsr.getValue(),
                tupl.tupl));
        InnoDB.ib_tuple_clear(tupl.tupl);
    }

    private void setValue(Tuple tupl, ColumnDef colDef, int i, Object val) {
        if (val == null) {
            if (colDef.getAttrs().contains(ColumnAttribute.NOT_NULL)) {
                throw new IllegalArgumentException(
                        "Cannot store null in non-null column: "
                                + colDef.getName());
            } else {
                Util.assertSuccess(InnoDB.ib_col_set_value(tupl.tupl, i,
                        Pointer.NULL, InnoDB.IB_SQL_NULL));
            }
        } else {
            switch (colDef.getType()) {
            case BINARY:
            case VARBINARY:
            case BLOB:
                byte[] byteVal = (byte[]) val;
                Util.assertSuccess(InnoDB.ib_col_set_value(tupl.tupl, i, Types
                        .getBytes(byteVal), byteVal.length));
                break;
            case CHAR:
            case CHAR_ANYCHARSET:
            case VARCHAR:
            case VARCHAR_ANYCHARSET:
                String stringVal = (String) val;
                Util.assertSuccess(InnoDB.ib_col_set_value(tupl.tupl, i, Types
                        .getString(stringVal), stringVal.length() + 1));
                break;
            case INT:
                Number numVal = (Number) val;
                switch (colDef.getLength()) {
                case 1:
                    Util.assertSuccess(InnoDB.ib_tuple_write_u8(tupl.tupl, i,
                            numVal.byteValue()));
                    break;
                case 2:
                    Util.assertSuccess(InnoDB.ib_tuple_write_u16(tupl.tupl, i,
                            numVal.shortValue()));
                    break;
                case 4:
                    Util.assertSuccess(InnoDB.ib_tuple_write_u32(tupl.tupl, i,
                            numVal.intValue()));
                    break;
                case 8:
                    Util.assertSuccess(InnoDB.ib_tuple_write_u64(tupl.tupl, i,
                            numVal.longValue()));
                    break;
                default:
                    throw new IllegalArgumentException(
                            "integer type not supported for length: "
                                    + colDef.getLength());
                }
                break;
            case DOUBLE:
                Number dubVal = (Number) val;
                Util.assertSuccess(InnoDB.ib_tuple_write_double(tupl.tupl, i,
                        dubVal.doubleValue()));
                break;
            case FLOAT:
                Number fltVal = (Number) val;
                Util.assertSuccess(InnoDB.ib_tuple_write_float(tupl.tupl, i,
                        fltVal.floatValue()));
                break;
            default:
                throw new IllegalArgumentException("unsupported type : "
                        + colDef.getType());
            }
        }
    }

    public void close() {
        Util.assertSuccess(InnoDB.ib_cursor_close(crsr.getValue()));
    }

    private static void assertCursorState(int err) {
        if (err != InnoDB.db_err.DB_SUCCESS
                && err != InnoDB.db_err.DB_END_OF_INDEX
                && err != InnoDB.db_err.DB_RECORD_NOT_FOUND) {
            throw new IllegalStateException("Cursor in invalid state (code "
                    + err + ")");
        }
    }
}
