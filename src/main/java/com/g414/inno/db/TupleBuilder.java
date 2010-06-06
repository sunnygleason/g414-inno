package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TupleBuilder {
    private final TableDef def;
    private final List<Object> values;
    private final AtomicInteger size = new AtomicInteger(0);

    public TupleBuilder(TableDef def) {
        this.def = def;
        this.values = new ArrayList<Object>(def.getColumnDefs().size());
    }

    public TupleBuilder addValue(Object value) {
        values.add(value);
        size.getAndIncrement();

        return this;
    }

    public List<Object> getValues() {
        return Collections.unmodifiableList(values);
    }

    public int getSize() {
        return size.get();
    }

    public TableDef getDef() {
        return def;
    }
}
