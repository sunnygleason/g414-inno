package com.g414.inno.db;

import java.util.Date;
import java.util.Random;

import org.testng.annotations.Test;

@Test
public class G414InnoDBTable1WriteTest {
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
            d.shutdown(false);
        }
    }

    private void insertRows(Database d) {
        Transaction t = d.beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_1);
        c.lock(LockMode.INTENTION_EXCLUSIVE);

        Random random = new Random();

        byte[] rand = new byte[16384];
        Tuple tupl = c.createClusteredIndexReadTuple();

        for (int i = 0; i < 10000; i++) {
            int j = 10 - i;

            String key = "hello" + i + " " + System.currentTimeMillis();
            String value = ("world" + j + " " + System.currentTimeMillis() + 17);
            random.nextBytes(rand);

            TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_1);

            r.addValues(key, value, Long.valueOf(j));
            r.addValues(System.nanoTime() * (1 + ((i % 2) * -2)));
            r.addValues(i % 2 == 1 ? null : Byte.valueOf((byte) 1));
            r.addValues(rand);

            c.insertRow(tupl, r);

            if (i % 1000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }

        tupl.delete();
        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }
}
