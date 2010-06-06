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

            createTable(d);

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

        Transaction t = d.beginTransaction(Level.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);

        try {
            Tuple search = c.createSearchTuple(new TupleBuilder(
                    G414InnoDBTableDefs.TABLE_2).addValue(key));

            SearchResultCode code = c.find(search, SearchMode.GE);
            System.out.println(code);
            Tuple row = c.createReadTuple();

            while (c.hasNext()) {
                c.readRow(row);

                byte[] theKey = row.getBytes(0);

                if (!Arrays.equals(key, theKey)) {
                    System.out.println("not found");
                    break;
                } else {
                    System.out.println("got it!");
                }

                byte[] thatVersionBytes = row.getBytes(1);

                // System.out.println("V: "
                // + new String(Hex.encodeHex(thisVersion)) + " "
                // + new String(Hex.encodeHex(thatVersionBytes)));

                System.out.println("before : deleting");
                c.deleteRow();

                row.clear();
                c.next();
            }

            TupleBuilder insert = new TupleBuilder(G414InnoDBTableDefs.TABLE_2);
            insert.addValue(key);
            insert.addValue(thisVersion);
            insert.addValue(value);

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
        Transaction t = d.beginTransaction(Level.REPEATABLE_READ);
        Cursor c = t.openTable(G414InnoDBTableDefs.TABLE_2);
        c.lock(Lock.INTENTION_EXCLUSIVE);

        Random random = new Random();

        Tuple tupl = c.createReadTuple();

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
