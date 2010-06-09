package com.g414.inno.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;

public class Tuple {
    protected final TableDef table;
    protected Pointer tupl;

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
                values.add(TupleStorage.loadBytes(this, i));
                break;
            case CHAR:
            case CHAR_ANYCHARSET:
            case VARCHAR:
            case VARCHAR_ANYCHARSET:
                values.add(TupleStorage.loadString(this, i));
                break;
            case INT:
                values.add(TupleStorage.loadInteger(this, i, def));
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
                values.put(def.getName(), TupleStorage.loadBytes(this, i));
                break;
            case CHAR:
            case CHAR_ANYCHARSET:
            case VARCHAR:
            case VARCHAR_ANYCHARSET:
                values.put(def.getName(), TupleStorage.loadString(this, i));
                break;
            case INT:
                values.put(def.getName(), TupleStorage.loadInteger(this, i, def));
                break;
            default:
                throw new IllegalArgumentException("unsupported datatype: "
                        + def.getType());
            }
        }

        return values;
    }

    public byte[] getBytes(int i) {
        return TupleStorage.loadBytes(this, i);
    }

    public String getString(int i) {
        return TupleStorage.loadString(this, i);
    }

    public Number getInteger(int i) {
        return TupleStorage.loadInteger(this, i, table.getColDefs().get(i));
    }

    public void clear() {
        tupl = InnoDB.ib_tuple_clear(tupl);
    }

    public void delete() {
        if (tupl != null && !tupl.equals(Pointer.NULL)) {
            InnoDB.ib_tuple_delete(tupl);
        }
    }
}
