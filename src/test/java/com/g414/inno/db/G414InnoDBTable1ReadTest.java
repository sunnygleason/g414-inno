package com.g414.inno.db;

import java.util.Date;

import org.testng.annotations.Test;


@Test
public class G414InnoDBTable1ReadTest {
    public void testInno() {
        Database d = new Database();

        readRows(d);

        d.shutdown(false);
    }

    private void readRows(Database d) {
        Transaction t = d.beginTransaction(Level.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_1);
        c.lock(Lock.INTENTION_EXCLUSIVE);
        c.first();

        Tuple tupl = c.createReadTuple();

        int i = 0;
        System.out.println(new Date() + " read...");

        while (c.hasNext()) {
            c.readRow(tupl);

            if (i % 50000 == 0) {
                System.out.println(tupl.valueMap());
                System.out.println(new Date() + " read " + i);
            }

            c.next();

            tupl.clear();
            i += 1;
        }
        System.out.println(new Date() + " read " + i);
        System.out.println(new Date() + " done.");

        c.close();
        t.commit();
    }
}
