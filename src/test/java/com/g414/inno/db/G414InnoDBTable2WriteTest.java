package com.g414.inno.db;

import java.util.Date;
import java.util.Random;

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

        Random random = new Random();

        Tuple tupl = c.createReadTuple();

        for (int i = 0; i < 40000; i++) {
            byte[] randKey = new byte[200];
            byte[] randVersion = new byte[200];
            byte[] randValue = new byte[1024];

            random.nextBytes(randKey);
            random.nextBytes(randVersion);
            random.nextBytes(randValue);

            TupleBuilder r = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);

            r.addValue(randKey);
            r.addValue(randVersion);
            r.addValue(randValue);

            c.insertRow(tupl, r);

            if (i % 10000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }

        tupl.delete();

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }

    private void createTable(Database d) {
        if (!d.tableExists(G414InnoDBTableDefs.TABLE_2)) {
            Schema s = d.createSchema(G414InnoDBTableDefs.TABLE_2_NAME,
                    TableType.DYNAMIC, 0);

            d.createTable(s, G414InnoDBTableDefs.TABLE_2);
            System.out.println("Created table: "
                    + G414InnoDBTableDefs.TABLE_2_NAME);
        }
    }
}
