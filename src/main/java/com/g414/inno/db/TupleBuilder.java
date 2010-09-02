package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TupleBuilder {
    public enum Options {
        COERCE, VALIDATE;
    }

    private final List<ColumnDef> columnDefs;
    private final List<Object> values;
    private final Set<Options> options;
    private int size = 0;

    public TupleBuilder(TableDef table, Options... values) {
        this.columnDefs = table.getColDefs();
        this.values = new ArrayList<Object>(columnDefs.size());
        this.options = getImmutableSetOf(values);
    }

    public static TupleBuilder fromValueMap(TableDef table,
            Map<String, Object> valueMap) {
        return fromValueMap(table, valueMap, Options.VALIDATE);
    }

    public static TupleBuilder fromValueMap(TableDef table,
            Map<String, Object> valueMap, Options... values) {
        TupleBuilder builder = new TupleBuilder(table, values);

        for (ColumnDef def : builder.columnDefs) {
            builder.addValues(valueMap.get(def.getName()));
        }

        return builder;
    }

    public TupleBuilder addValue(Object value) {
        if (size >= columnDefs.size()) {
            throw new IllegalStateException("tuple already full!");
        }

        ColumnDef def = columnDefs.get(size);

        if (options.contains(Options.COERCE) && value != null
                && value instanceof String) {
            value = TupleStorage.coerceType((String) value, def.getType());
        }

        if (options.contains(Options.VALIDATE)) {
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

    private static Set<Options> getImmutableSetOf(Options... values) {
        EnumSet<Options> newOptions = EnumSet.noneOf(Options.class);

        for (Options opt : values) {
            newOptions.add(opt);
        }

        return Collections.unmodifiableSet(newOptions);
    }
}
