package com.g414.inno.db;

import java.util.ArrayList;
import java.util.List;

public class TupleBuilder {
    public List<ColumnValue> values;

    public TupleBuilder() {
        this.values = new ArrayList<ColumnValue>();
    }

    public TupleBuilder addValue(Integer colIndex, Object value, Integer length) {
        this.values.add(new ColumnValue(colIndex, value, length));

        return this;
    }

    public List<ColumnValue> getValues() {
        return values;
    }
}
