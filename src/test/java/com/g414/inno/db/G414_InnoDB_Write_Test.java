package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.testng.annotations.Test;

import com.g414.inno.db.Cursor.Lock;
import com.g414.inno.db.Transaction.Level;

@Test
public class G414_InnoDB_Write_Test {
    private static final String DB_NAME = "foo";
    private static final String TABLE_NAME = "foo/bar";

    public void testInno() {
        Database d = new Database();
        try {
            d.createDatabase(DB_NAME);

            createTable(d);

            insertRows(d);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            d.dropDatabase(DB_NAME + "/");
            d.shutdown(false);
        }
    }

    private void insertRows(Database d) {
        Transaction t = d.beginTransaction(Level.REPEATABLE_READ);
        Cursor c = t.openTableByName(TABLE_NAME);
        c.lock(Lock.INTENTION_EXCLUSIVE);

        List<TupleBuilder> list = new ArrayList<TupleBuilder>();

        for (int i = 0; i < 100000; i++) {
            String key = "hello" + i + " " + System.currentTimeMillis();
            String value = ("world" + i + " " + System.currentTimeMillis() + 17);
            TupleBuilder r = new TupleBuilder();

            r.addValue(0, key, key.length() + 1);
            r.addValue(1, value, value.length() + 1);
            r.addValue(2, Long.valueOf(i), 8);

            list.add(r);

            if (i % 10000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }
        System.out.println(new Date() + " start -");

        c.insertRows(list);

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }

    private void createTable(Database d) {
        if (!d.tableExists(TABLE_NAME)) {
            Schema s = d.createSchema(TABLE_NAME, TableType.COMPRESSED, 0);

            TableBuilder b = new TableBuilder(TABLE_NAME);
            b.addColumn(new ColumnDef("c1", ColumnType.VARCHAR, 32));
            b.addColumn(new ColumnDef("c2", ColumnType.VARCHAR, 32));
            b.addColumn(new ColumnDef("c3", ColumnType.INT, 8));

            b.addIndex("c1_c2", "c1", 0, true, true);
            b.addIndex("c1_c2", "c2", 0, true, true);

            Table tbl = d.createTable(s, b);
            System.out.println("Created table: " + TABLE_NAME + " ("
                    + tbl.getId() + ")");
        }
    }
}
