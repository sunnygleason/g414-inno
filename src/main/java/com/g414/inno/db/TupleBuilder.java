package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class TupleBuilder {
    private final List<ColumnDef> columnDefs;
    private final List<Object> values;
    private final boolean validate;
    private int size = 0;

    public TupleBuilder(TableDef table) {
        this(table, true);
    }

    public TupleBuilder(TableDef table, boolean validate) {
        this.columnDefs = table.getColDefs();
        this.values = new ArrayList<Object>(columnDefs.size());
        this.validate = validate;
    }

    public static TupleBuilder fromValueMap(TableDef table,
            Map<String, Object> valueMap) {
        return fromValueMap(table, valueMap, true);
    }

    public static TupleBuilder fromValueMap(TableDef table,
            Map<String, Object> valueMap, boolean validate) {
        TupleBuilder builder = new TupleBuilder(table, validate);

        for (ColumnDef def : builder.columnDefs) {
            builder.addValues(valueMap.get(def.getName()));
        }

        return builder;
    }

    public TupleBuilder addValue(Object value) {
        if (size >= columnDefs.size()) {
            throw new IllegalStateException("tuple already full!");
        }

        if (validate) {
            ColumnDef def = columnDefs.get(size);
            if (!Validation.isValid(def, value)) {
                throw new InnoException("Invalid object for column=" + size
                        + ", type=" + def.getType() + ", value=" + value);
            }
        }

        values.add(value);
        size += 1;

        return this;
    }

    public TupleBuilder addValues(Object... valuez) {
        for (Object value : valuez) {
            this.addValue(value);
        }

        return this;
    }

    public List<Object> getValues() {
        return Collections.unmodifiableList(values);
    }

    public int getSize() {
        return size;
    }

    public List<ColumnDef> getColumnDefs() {
        return columnDefs;
    }
}
