package com.g414.inno.db;

import com.sun.jna.ptr.PointerByReference;

public class Schema {
    private final PointerByReference schema;

    public Schema(PointerByReference schema) {
        this.schema = schema;
    }

    public PointerByReference getSchema() {
        return schema;
    }
}
