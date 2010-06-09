package com.g414.inno.db;

import java.util.Date;

import org.testng.annotations.Test;

@Test
public class G414InnoDBTable2WriteTest {
    private static final String DB_NAME = "foo";

    public void testInno() {
        Database d = new Database();
        try {
            d.createDatabase(DB_NAME);

            createTable(d);

            insertRows(d);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // d.dropDatabase(DB_NAME + "/");
            d.shutdown(false);
        }
    }

    private void insertRows(Database d) {
        Transaction t = d.beginTransaction(Level.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.lock(Lock.INTENTION_EXCLUSIVE);

        Tuple tupl = c.createReadTuple();

        for (int i = 0; i < 10000; i++) {
            byte[] randKey = ("hi_" + i).getBytes();
            byte[] randVersion = Integer.toString(i).getBytes();

            byte[] randValue = ("wrld_" + i).getBytes();
            TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

            r.addValue(randKey);
            r.addValue(randVersion);
            r.addValue(randValue);

            c.insertRow(tupl, r);

            if (i % 1000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }

        TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

        r.addValue(new byte[0]);
        r.addValue("hi1".getBytes());
        r.addValue(new byte[0]);

        c.insertRow(tupl, r);

        r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

        r.addValue(new byte[0]);
        r.addValue("hi2".getBytes());
        r.addValue(null);

        c.insertRow(tupl, r);

        tupl.delete();

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }

    private void createTable(Database d) {
        if (!d.tableExists(G414InnoDBTableDefs.TABLE_2)) {
            d.createTable(G414InnoDBTableDefs.TABLE_2);
            System.out.println("Created table: "
                    + G414InnoDBTableDefs.TABLE_2_NAME);
        }
    }
}
