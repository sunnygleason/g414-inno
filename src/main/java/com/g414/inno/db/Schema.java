package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.ptr.PointerByReference;

public class Schema {
    private final PointerByReference schema;

    public Schema(PointerByReference schema) {
        this.schema = schema;
    }

    public PointerByReference getSchema() {
        return schema;
    }

    public void close() {
        InnoDB.ib_table_schema_delete(schema.getValue());
    }
}
