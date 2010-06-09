package com.g414.inno.db;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

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
            setValue(searchTuple, colDef, i, value);
        }

        return searchTuple;
    }

    public SearchResultCode find(Tuple tupl, SearchMode searchMode,
            boolean clusterAccess) {
        IntBuffer result = ByteBuffer.allocateDirect(4).asIntBuffer();
        err = InnoDB.ib_cursor_moveto(crsr.getValue(), tupl.tupl, searchMode
                .getCode(), result);
        if (clusterAccess) {
            InnoDB.ib_cursor_set_cluster_access(crsr.getValue());
        }

        assertCursorState(err);

        return SearchResultCode.fromCode(result.get());
    }

    public void readRow(Tuple tupl) {
        if (!this.hasNext()) {
            throw new IllegalStateException("no row at cursor!");
        }
        err = InnoDB.ib_cursor_read_row(crsr.getValue(), tupl.tupl);
        assertCursorState(err);
    }

    public boolean isPositioned() {
        return InnoDB.ib_cursor_is_positioned(crsr.getValue()) == InnoDB.IB_TRUE;
    }

    public void deleteRow() {
        err = InnoDB.ib_cursor_delete_row(crsr.getValue());
        assertCursorState(err);
    }

    public void first() {
        err = InnoDB.ib_cursor_first(crsr.getValue());
        assertCursorState(err);
    }

    public void last() {
        err = InnoDB.ib_cursor_last(crsr.getValue());
        assertCursorState(err);
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
            tuple.delete();
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
            tuple.delete();
        }
    }

    public void insertRow(Tuple tupl, TupleBuilder tuple) {
        List<Object> values = tuple.getValues();
        TableDef def = tuple.getDef();
        List<ColumnDef> colDefs = def.getColDefs();

        int len = colDefs.size();

        for (int i = 0; i < len; i++) {
            Object val = values.get(i);
            ColumnDef colDef = colDefs.get(i);

            setValue(tupl, colDef, i, val);
        }

        Util.assertSuccess(InnoDB.ib_cursor_insert_row(crsr.getValue(),
                tupl.tupl));

        tupl.clear();
    }

    private static void setValue(Tuple tupl, ColumnDef colDef, int i, Object val) {
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
                TupleStorage.storeBytes(tupl, i, (byte[]) val);
                break;
            case CHAR:
            case CHAR_ANYCHARSET:
            case VARCHAR:
            case VARCHAR_ANYCHARSET:
                TupleStorage.storeString(tupl, i, (String) val);
                break;
            case INT:
                Number numVal = (Number) val;
                TupleStorage.storeInteger(tupl, colDef, i, numVal);
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

    public void reset() {
        Util.assertSuccess(InnoDB.ib_cursor_reset(crsr.getValue()));
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
