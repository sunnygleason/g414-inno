package com.g414.inno.db;

import org.testng.annotations.Test;

@Test
public class G414InnoDBCreateDropDBTest {
    private static String DATABASE_NAME = "foo";
    private static String SCHEMA_NAME = "foo/bar";

    public void testCreateDrop() throws Exception {
        Database d = new Database();

        d.dropDatabase(DATABASE_NAME + "/");

        d.createDatabase(DATABASE_NAME);

        Schema s = d.createSchema(SCHEMA_NAME, TableType.COMPRESSED, 0);

        TableBuilder b = new TableBuilder("whoa");
        b.addColumn("c1", ColumnType.INT, 4);
        d.createTable(s, b.build());
        s.close();

        Thread.sleep(10000);
        d.dropDatabase(DATABASE_NAME + "/");
    }
}
