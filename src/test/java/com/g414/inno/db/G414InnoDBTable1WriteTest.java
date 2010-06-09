package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.testng.annotations.Test;

@Test
public class G414InnoDBTable1WriteTest {
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
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_1);
        c.lock(Lock.INTENTION_EXCLUSIVE);

        Random random = new Random();

        byte[] rand = new byte[16384];
        Tuple tupl = c.createReadTuple();

        for (int i = 0; i < 1000; i++) {
            String key = "hello" + i + " " + System.currentTimeMillis();
            String value = ("world" + i + " " + System.currentTimeMillis() + 17);
            random.nextBytes(rand);

            TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_1);

            r.addValue(key);
            r.addValue(value);
            r.addValue(Long.valueOf(i));
            r.addValue(System.nanoTime() * (1 + ((i % 2) * -2)));
            r.addValue(i % 2 == 1 ? null : Byte.valueOf((byte) 1));
            r.addValue(rand);

            c.insertRow(tupl, r);

            if (i % 10000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }

        tupl.delete();
        System.out.println(new Date() + " start -");

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }

    private void createTable(Database d) {
        if (!d.tableExists(G414InnoDBTableDefs.TABLE_1)) {
            d.createTable(G414InnoDBTableDefs.TABLE_1);
            System.out.println("Created table: "
                    + G414InnoDBTableDefs.TABLE_1_NAME);
        }
    }
}
