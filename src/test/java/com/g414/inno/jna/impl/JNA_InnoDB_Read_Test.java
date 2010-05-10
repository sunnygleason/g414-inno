package com.g414.inno.jna.impl;

import java.nio.IntBuffer;

import org.testng.annotations.Test;

import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

@Test
public class JNA_InnoDB_Read_Test {
    private static final String TABLE_NAME = "foo/bar";

    public void testInno() {
        showAssert(InnoDB.ib_init());
        showAssert(InnoDB.ib_startup("barracuda"));

        readRows();

        showAssert(InnoDB
                .ib_shutdown(InnoDB.ib_shutdown_t.IB_SHUTDOWN_NORMAL));
    }

    private void readRows() {
        Pointer trx = InnoDB
                .ib_trx_begin(InnoDB.ib_trx_level_t.IB_TRX_REPEATABLE_READ);

        PointerByReference ib_crsr = new PointerByReference();

        showAssert(InnoDB.ib_cursor_open_table(TABLE_NAME, trx, ib_crsr));
        showAssert(InnoDB.ib_cursor_lock(ib_crsr.getValue(),
                InnoDB.ib_lck_mode_t.IB_LOCK_IX));

        showAssert(InnoDB.ib_cursor_first(ib_crsr.getValue()));
        Pointer tupl = InnoDB.ib_clust_read_tuple_create(ib_crsr.getValue());
        
        int i = 0;
        int err = InnoDB.db_err.DB_SUCCESS;
        
        while (err == InnoDB.db_err.DB_SUCCESS) {
            err = InnoDB.ib_cursor_read_row(ib_crsr.getValue(), tupl);
            
            assert (err == InnoDB.db_err.DB_SUCCESS
                    || err == InnoDB.db_err.DB_END_OF_INDEX
                    || err == InnoDB.db_err.DB_RECORD_NOT_FOUND);
            
            System.out.println("ROW " + i++);
            
            Pointer v1 = InnoDB.ib_col_get_value(tupl, 0);
            System.out.println(v1.getString(0));
            Pointer v2 = InnoDB.ib_col_get_value(tupl, 1);
            System.out.println(v2.getString(0));
            IntBuffer v3 = IntBuffer.allocate(1);
            showAssert(InnoDB.ib_tuple_read_u32(tupl, 2, v3));
            System.out.println(v3.get());
            
            err = InnoDB.ib_cursor_next(ib_crsr.getValue());
            InnoDB.ib_tuple_clear(tupl);
        }

        showAssert(InnoDB.ib_cursor_close(ib_crsr.getValue()));
        showAssert(InnoDB.ib_trx_commit(trx));
    }

    private void showAssert(int value) {
        if (InnoDB.db_err.DB_SUCCESS != value) {
            Exception e = new Exception();
            System.out.println(InnoDB.ib_strerror(value).getString(0));
            e.printStackTrace();
        }
    }
}
