package com.g414.inno.db;

import java.nio.LongBuffer;
import java.util.Date;

import org.testng.annotations.Test;

import com.g414.inno.db.Cursor.Lock;
import com.g414.inno.db.Transaction.Level;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;

@Test
public class G414_InnoDB_Read_Test {
    private static final String TABLE_NAME = "foo/bar";

    public void testInno() {
        Database d = new Database();

        readRows(d);

        d.shutdown(false);
    }

    private void readRows(Database d) {
        Transaction t = d.beginTransaction(Level.REPEATABLE_READ);
        Cursor c = t.openTableByName(TABLE_NAME);
        c.lock(Lock.INTENTION_EXCLUSIVE);

        c.first();

        Pointer tupl = InnoDB
                .ib_clust_read_tuple_create(c.getCrsr().getValue());

        int i = 0;
        int err = InnoDB.db_err.DB_SUCCESS;

        System.out.println(new Date() + " read...");

        while (err == InnoDB.db_err.DB_SUCCESS) {
            err = InnoDB.ib_cursor_read_row(c.getCrsr().getValue(), tupl);
            assert (err == InnoDB.db_err.DB_SUCCESS
                    || err == InnoDB.db_err.DB_END_OF_INDEX || err == InnoDB.db_err.DB_RECORD_NOT_FOUND);

            // System.out.println("---ROW " + i++);

            Pointer v1 = InnoDB.ib_col_get_value(tupl, 0);
            String vv1 = v1.getString(0);
            Pointer v2 = InnoDB.ib_col_get_value(tupl, 1);
            String vv2 = v2.getString(0);
            LongBuffer v3 = LongBuffer.allocate(1);
            showAssert(InnoDB.ib_tuple_read_u64(tupl, 2, v3));
            Long vv3 = v3.get();

            if (i % 50000 == 0) {
                System.out.println(new Date() + " read " + i);
            }
            err = InnoDB.ib_cursor_next(c.getCrsr().getValue());
            InnoDB.ib_tuple_clear(tupl);
            i += 1;
        }
        System.out.println(new Date() + " read " + i);

        System.out.println(new Date() + " done.");
        c.close();
        t.commit();
    }

    private void showAssert(int value) {
        if (InnoDB.db_err.DB_SUCCESS != value) {
            Exception e = new Exception();
            System.out.println(InnoDB.ib_strerror(value).getString(0));
            e.printStackTrace();
        }
    }
}
