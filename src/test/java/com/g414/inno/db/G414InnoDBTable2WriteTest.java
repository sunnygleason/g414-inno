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

            G414InnoDBTableDefs.createTables(d);

            insertRows(d);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // d.dropDatabase(DB_NAME + "/");
            d.shutdown(false);
        }
    }

    private void insertRows(Database d) {
        Transaction t = d.beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.lock(LockMode.INTENTION_EXCLUSIVE);

        Tuple tupl = c.createClusteredIndexReadTuple();

        for (int i = 0; i < 10000; i++) {
            byte[] randKey = ("hi_" + i).getBytes();
            byte[] randVersion = Integer.toString(i).getBytes();

            byte[] randValue = ("wrld_" + i).getBytes();
            TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

            r.addValues(randKey, randVersion, randValue);

            c.insertRow(tupl, r);

            if (i % 1000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }

        TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

        r.addValues(new byte[0]);
        r.addValues("hi1".getBytes());
        r.addValues(new byte[0]);

        c.insertRow(tupl, r);

        r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

        r.addValues(new byte[0]);
        r.addValues("hi2".getBytes());
        r.addValues((Object) null);

        c.insertRow(tupl, r);

        tupl.delete();

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }
}
