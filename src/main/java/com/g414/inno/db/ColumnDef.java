package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ColumnDef {
    private final String name;
    private final ColumnType type;
    private final List<ColumnAttribute> attrs;
    private final Integer length;

    public ColumnDef(String name, ColumnType type, Integer length,
            ColumnAttribute... attrs) {
        this.name = name;
        this.type = type;
        this.length = length;
        List<ColumnAttribute> newAttrs = new ArrayList<ColumnAttribute>();
        newAttrs.addAll(Arrays.asList(attrs));
        this.attrs = Collections.unmodifiableList(newAttrs);
    }

    public String getName() {
        return name;
    }

    public ColumnType getType() {
        return type;
    }

    public List<ColumnAttribute> getAttrs() {
        return attrs;
    }

    public Integer getLength() {
        return length;
    }
}
