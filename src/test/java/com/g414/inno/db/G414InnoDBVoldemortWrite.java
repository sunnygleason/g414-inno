package com.g414.inno.db;

import java.util.Arrays;
import java.util.Date;
import java.util.Random;

import org.apache.commons.codec.binary.Base64;
import org.testng.annotations.Test;

@Test
public class G414InnoDBVoldemortWrite {
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

    private void insert(Database d, byte[] key, byte[] thisVersion, byte[] value) {
        System.out.println("put() : " + new String(key));

        Transaction t = d.beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.setClusterAccess();

        try {
            Tuple search = c.createClusteredIndexSearchTuple(new TupleBuilder(
                    G414InnoDBTableDefs.TABLE_2).addValues(key));

            SearchResultCode code = c.find(search, SearchMode.GE);
            System.out.println(code);
            Tuple row = c.createClusteredIndexReadTuple();

            while (c.hasNext()) {
                c.readRow(row);

                byte[] theKey = row.getBytes(0);

                if (!Arrays.equals(key, theKey)) {
                    System.out.println("not found");
                    break;
                } else {
                    System.out.println("got it!");
                }

                System.out.println("before : deleting");
                c.deleteRow();

                row.clear();
                c.next();
            }

            TupleBuilder insert = new TupleBuilder(G414InnoDBTableDefs.TABLE_2)
                    .addValues(key, thisVersion, value);

            System.out.println("inserting");
            c.insertRow(row, insert);
            row.delete();
            search.delete();
        } finally {
            c.close();
            t.commit();
        }
    }

    private void insertRows(Database d) {
        Transaction t = d.beginTransaction(TransactionLevel.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.lock(LockMode.INTENTION_EXCLUSIVE);

        Random random = new Random();

        Tuple tupl = c.createClusteredIndexReadTuple();

        for (int i = 0; i < 300; i++) {
            byte[] randKey = new byte[1];
            byte[] randVersion = new byte[10];
            byte[] randValue = new byte[1024];

            random.nextBytes(randKey);
            random.nextBytes(randVersion);
            random.nextBytes(randValue);

            insert(d, Base64.encodeBase64(randKey), Base64
                    .encodeBase64(randVersion), Base64.encodeBase64(randValue));

            if (i % 10000 == 0) {
                System.out.println(new Date() + " make row " + i);
            }
        }

        tupl.delete();

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }
}
