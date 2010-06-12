package com.g414.inno.db;

import java.util.Date;

import org.testng.annotations.Test;

@Test
public class G414InnoDBTable2ReadTest {
    public void testInno() {
        Database d = new Database();

        readRows(d);

        d.shutdown(false);
    }

    private void readRows(Database d) {
        Transaction t = d.beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.lock(LockMode.INTENTION_EXCLUSIVE);

        c.first();

        Tuple tupl = c.createClusteredIndexReadTuple();

        int i = 0;
        System.out.println(new Date() + " read...");

        while (c.hasNext()) {
            if (i % 1000 == 0) {
                c.readRow(tupl);
                System.out.println(tupl.values());
                for (Object val : tupl.values()) {
                    System.out.println(new String((byte[]) val));
                }

                System.out.println(tupl.valueMap());
                System.out.println(new Date() + " read " + i);
            }

            tupl.clear();
            c.next();
            i += 1;
        }
        c.close();

        c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.first();

        i = 0;
        while (c.hasNext()) {
            if (i % 10000 == 0) {
                c.readRow(tupl);
                System.out.println(tupl.valueMap());
                System.out.println(new Date() + " reread " + i);
            }

            c.next();
            tupl.clear();
            i += 1;
        }

        tupl.delete();

        System.out.println(new Date() + " read " + i);
        System.out.println(new Date() + " done.");

        c.close();
        t.commit();
    }
}
