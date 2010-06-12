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
        {
            Transaction t = d
                    .beginTransaction(TransactionLevel.REPEATABLE_READ);
            Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_1);
            c.last();

            Tuple tupl = c.createClusteredIndexReadTuple();

            int i = 0;
            System.out.println(new Date() + " read...");

            while (c.hasNext()) {
                c.readRow(tupl);

                // if (i % 50000 == 0) {
                System.out.println(tupl.valueMap());
                // System.out.println(new Date() + " read " + i);
                // }

                c.prev();

                tupl.clear();
                i += 1;
            }
            System.out.println(new Date() + " read " + i);
            System.out.println(new Date() + " done.");

            tupl.delete();

            c.close();
            t.commit();
        }
        {
            Transaction t = d
                    .beginTransaction(TransactionLevel.REPEATABLE_READ);
            Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_1);

            Cursor sec = c.openIndex("c3");
            sec.last();

            int i = 0;
            System.out.println(new Date() + " read sec...");
            Tuple tupl = sec.createSecondaryIndexReadTuple();

            while (sec.hasNext()) {
                sec.readRow(tupl);

                if (i % 50000 == 0) {
                    System.out.println(tupl.valueMap());
                    System.out.println(new Date() + " read " + i);
                }

                sec.prev();

                tupl.clear();
                i += 1;
            }
            System.out.println(new Date() + " read " + i);
            System.out.println(new Date() + " done.");

            tupl.delete();

            sec.close();
            c.close();
            t.commit();
        }

    }
}
