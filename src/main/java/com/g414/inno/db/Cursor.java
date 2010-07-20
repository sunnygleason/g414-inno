package com.g414.inno.db;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Cursor {
    private final PointerByReference crsr;
    private final TableDef table;
    private final IndexDef index;

    private volatile int err = InnoDB.db_err.DB_SUCCESS;

    public Cursor(PointerByReference crsr, TableDef table, IndexDef index) {
        this.crsr = crsr;
        this.table = table;
        this.index = index;
    }

    public PointerByReference getCrsr() {
        return crsr;
    }

    public Tuple createClusteredIndexReadTuple() {
        if (this.index != null) {
            throw new IllegalArgumentException(
                    "Secondary index cursor may not create cluster read tuples");
        }

        return new Tuple(InnoDB.ib_clust_read_tuple_create(crsr.getValue()),
                table.getColDefs());
    }

    public Tuple createClusteredIndexSearchTuple(TupleBuilder tuple) {
        if (this.index != null) {
            throw new IllegalArgumentException(
                    "Secondary index cursor may not create cluster search tuples");
        }

        Tuple searchTuple = new Tuple(InnoDB.ib_clust_search_tuple_create(crsr
                .getValue()), table.getColDefs());

        List<Object> values = tuple.getValues();

        for (int i = 0; i < tuple.getSize(); i++) {
            Object value = values.get(i);
            ColumnDef colDef = table.getColDefs().get(i);
            setValue(searchTuple, colDef, i, value);
        }

        return searchTuple;
    }

    public Tuple createSecondaryIndexReadTuple() {
        if (this.index == null) {
            throw new IllegalArgumentException(
                    "Clustered index cursor may not create secondary index read tuples");
        }

        return new Tuple(InnoDB.ib_sec_read_tuple_create(crsr.getValue()),
                index.getColumns());
    }

    public Tuple createSecondaryIndexSearchTuple(TupleBuilder tuple) {
        if (this.index == null) {
            throw new IllegalArgumentException(
                    "Clustered index cursor may not create secondary index search tuples");
        }

        Tuple searchTuple = new Tuple(InnoDB.ib_sec_search_tuple_create(crsr
                .getValue()), index.getColumns());

        List<Object> values = tuple.getValues();

        for (int i = 0; i < tuple.getSize(); i++) {
            Object value = values.get(i);
            ColumnDef colDef = index.getColumns().get(i);
            setValue(searchTuple, colDef, i, value);
        }

        return searchTuple;
    }

    public Cursor openIndex(String indexName) {
        if (this.index != null) {
            throw new IllegalArgumentException(
                    "cannot open index from a secondary index cursor");
        }

        if (!this.table.getIndexDefs().containsKey(indexName)) {
            throw new IllegalArgumentException("unknown index: " + indexName);
        }

        PointerByReference indexCrsr = new PointerByReference();

        Util.assertSuccess(InnoDB.ib_cursor_open_index_using_name(crsr
                .getValue(), indexName, indexCrsr));

        return new Cursor(indexCrsr, table, table.getIndexDefs().get(indexName));
    }

    public void setClusterAccess() {
        InnoDB.ib_cursor_set_cluster_access(crsr.getValue());
    }

    public SearchResultCode find(Tuple tupl, SearchMode searchMode) {
        IntBuffer result = ByteBuffer.allocateDirect(4).asIntBuffer();
        err = InnoDB.ib_cursor_moveto(crsr.getValue(), tupl.tupl, searchMode
                .getCode(), result);

        assertCursorState(err);

        return SearchResultCode.fromCode(result.get());
    }

    public void readRow(Tuple tupl) {
        if (!this.isPositioned()) {
            throw new IllegalStateException("no row at cursor!");
        }

        err = InnoDB.ib_cursor_read_row(crsr.getValue(), tupl.tupl);
        assertCursorState(err);
    }

    public boolean hasNext() {
        return (err == InnoDB.db_err.DB_SUCCESS);
    }

    public boolean isPositioned() {
        return (InnoDB.ib_cursor_is_positioned(crsr.getValue()) == InnoDB.IB_TRUE);
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

    public void prev() {
        err = InnoDB.ib_cursor_prev(crsr.getValue());
        assertCursorState(err);
    }

    public void next() {
        err = InnoDB.ib_cursor_next(crsr.getValue());
        assertCursorState(err);
    }

    public void lock(LockMode mode) {
        Util.assertSuccess(InnoDB.ib_cursor_lock(crsr.getValue(), mode
                .getCode()));
    }

    public void setLockMode(LockMode mode) {
        Util.assertSuccess(InnoDB.ib_cursor_set_lock_mode(crsr.getValue(), mode
                .getCode()));
    }

    public void insertRow(Tuple tupl, TupleBuilder tuple) {
        List<Object> values = tuple.getValues();
        List<ColumnDef> colDefs = tuple.getColumnDefs();

        if (values.size() != tupl.columns.size()) {
            throw new IllegalArgumentException("Must specify all column values");
        }

        try {
            for (int i = 0; i < values.size(); i++) {
                Object val = values.get(i);
                ColumnDef def = colDefs.get(i);

                setValue(tupl, def, i, val);
            }

            Util.assertSuccess(InnoDB.ib_cursor_insert_row(crsr.getValue(),
                    tupl.tupl));
        } finally {
            tupl.clear();
        }
    }

    public void updateRow(Tuple oldTuple, TupleBuilder tuple) {
        List<Object> values = tuple.getValues();
        List<ColumnDef> colDefs = tuple.getColumnDefs();

        if (values.size() != oldTuple.columns.size()) {
            throw new IllegalArgumentException("Must specify all column values");
        }

        Tuple newTuple = this.createClusteredIndexReadTuple();

        try {
            Util.assertSuccess(InnoDB.ib_tuple_copy(newTuple.tupl,
                    oldTuple.tupl));

            for (int i = 0; i < colDefs.size(); i++) {
                Object val = values.get(i);
                ColumnDef def = colDefs.get(i);

                setValue(newTuple, def, i, val);
            }

            Util.assertSuccess(InnoDB.ib_cursor_update_row(crsr.getValue(),
                    oldTuple.tupl, newTuple.tupl));
        } finally {
            oldTuple.clear();
            newTuple.delete();
        }
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
