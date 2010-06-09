package com.g414.inno.db;

public class G414InnoDBTableDefs {
    public static final String SCHEMA_NAME = "foo";
    
    public static final String TABLE_1_NAME = "foo/bar";
    public static final TableDef TABLE_1;

    public static final String TABLE_2_NAME = "foo/vtest";
    public static final TableDef TABLE_2;

    static {
        TableBuilder b1 = new TableBuilder(TABLE_1_NAME);
        b1.addColumn("c1", ColumnType.VARCHAR, 32);
        b1.addColumn("c2", ColumnType.VARCHAR, 32);
        b1.addColumn("c3", ColumnType.INT, 8);
        b1.addColumn("c4", ColumnType.INT, 8);
        b1.addColumn("c5", ColumnType.INT, 4);
        b1.addColumn("c6", ColumnType.BLOB, 0);

        b1.addIndex("c1_c2", "c1", 0, true, true);
        b1.addIndex("c1_c2", "c2", 0, true, true);
        TABLE_1 = b1.build();

        TableBuilder b2 = new TableBuilder(TABLE_2_NAME);
        b2.addColumn("key_", ColumnType.VARBINARY, 200,
                ColumnAttribute.NOT_NULL);
        b2.addColumn("version_", ColumnType.VARBINARY, 200,
                ColumnAttribute.NOT_NULL);
        b2.addColumn("value_", ColumnType.BLOB, 0);

        b2.addIndex("PRIMARY", "key_", 0, true, true);
        b2.addIndex("PRIMARY", "version_", 0, true, true);

        TABLE_2 = b2.build();
    }
}
