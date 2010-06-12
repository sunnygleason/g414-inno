package com.g414.inno.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;

public class Tuple {
    protected final List<ColumnDef> columns;
    protected Pointer tupl;
    private final int size;
    private boolean deleted = false;

    public Tuple(Pointer tupl, List<ColumnDef> columns) {
        this.tupl = tupl;
        this.columns = columns;
        this.size = columns.size();
    }

    public List<Object> values() {
        if (deleted) {
            throw new IllegalStateException("tuple already deleted!");
        }

        List<Object> values = new ArrayList<Object>(this.columns.size());

        for (ColumnDef def : this.columns) {
            values.add(getValue(def));
        }

        return values;
    }

    public Map<String, Object> valueMap() {
        if (deleted) {
            throw new IllegalStateException("tuple already deleted!");
        }

        Map<String, Object> values = new LinkedHashMap<String, Object>(
                this.columns.size());

        for (ColumnDef def : this.columns) {
            values.put(def.getName(), this.getValue(def));
        }

        return values;
    }

    public byte[] getBytes(int i) {
        if (deleted) {
            throw new IllegalStateException("tuple already deleted!");
        }

        if (i >= this.size) {
            throw new IndexOutOfBoundsException("invalid index: " + i);
        }

        ColumnDef def = this.columns.get(i);
        if (!def.getType().isByteArrayType()) {
            throw new IllegalArgumentException("invalid column "
                    + def.getName() + ", not byte[]: " + i);
        }

        return TupleStorage.loadBytes(this, i);
    }

    public String getString(int i) {
        if (deleted) {
            throw new IllegalStateException("tuple already deleted!");
        }

        if (i >= this.size) {
            throw new IndexOutOfBoundsException("invalid index: " + i);
        }

        ColumnDef def = this.columns.get(i);
        if (def.getType().isStringType()) {
            throw new IllegalArgumentException("invalid column "
                    + def.getName() + ", not String: " + i);
        }

        return TupleStorage.loadString(this, i);
    }

    public Number getInteger(int i) {
        if (deleted) {
            throw new IllegalStateException("tuple already deleted!");
        }

        if (i >= this.size) {
            throw new IndexOutOfBoundsException("invalid index: " + i);
        }

        ColumnDef def = this.columns.get(i);
        if (!def.getType().isIntegerType()) {
            throw new IllegalArgumentException("invalid column "
                    + def.getName() + ", not integer type: " + i);
        }

        return TupleStorage.loadInteger(this, i, def.getLength(), !def
                .is(ColumnAttribute.UNSIGNED));
    }

    public void clear() {
        if (deleted) {
            throw new IllegalStateException("tuple is deleted!");
        }

        tupl = InnoDB.ib_tuple_clear(tupl);
    }

    public void delete() {
        if (deleted) {
            throw new IllegalStateException("tuple is deleted!");
        }

        if (tupl != null && !tupl.equals(Pointer.NULL)) {
            InnoDB.ib_tuple_delete(tupl);
        }
    }

    private Object getValue(ColumnDef def) {
        switch (def.getType()) {
        case BINARY:
        case VARBINARY:
        case BLOB:
            return TupleStorage.loadBytes(this, def.getIndex());
        case CHAR:
        case CHAR_ANYCHARSET:
        case VARCHAR:
        case VARCHAR_ANYCHARSET:
            return TupleStorage.loadString(this, def.getIndex());
        case INT:
            return TupleStorage.loadInteger(this, def.getIndex(), def
                    .getLength(), !def.getAttrs().contains(
                    ColumnAttribute.UNSIGNED));
        default:
            throw new IllegalArgumentException("unsupported datatype: "
                    + def.getType());
        }
    }
}
