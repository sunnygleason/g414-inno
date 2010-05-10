package com.g414.inno.jna.impl;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;

import org.testng.annotations.Test;

import com.sun.jna.Memory;
import com.sun.jna.Native;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

@Test
public class JNA_InnoDB_Write_Test {
    private static final String DB_NAME = "foo";
    private static final String TABLE_NAME = "foo/bar";

    public void testInno() {
        showAssert(InnoDB.ib_init());
        showAssert(InnoDB.ib_startup("barracuda"));
        showAssert(InnoDB.ib_database_create(DB_NAME));

        createTable();

        insertRows();

        showAssert(InnoDB.ib_shutdown(InnoDB.ib_shutdown_t.IB_SHUTDOWN_NORMAL));
    }

    private void createTable() {
        PointerByReference ib_tbl_sch = new PointerByReference();
        showAssert(InnoDB.ib_table_schema_create(TABLE_NAME, ib_tbl_sch,
                InnoDB.ib_tbl_fmt_t.IB_TBL_COMPRESSED, 0));

        showAssert(InnoDB.ib_table_schema_add_col(ib_tbl_sch.getValue(), "c1",
                InnoDB.ib_col_type_t.IB_VARCHAR,
                InnoDB.ib_col_attr_t.IB_COL_NONE, (short) 0, 32));
        showAssert(InnoDB.ib_table_schema_add_col(ib_tbl_sch.getValue(), "c2",
                InnoDB.ib_col_type_t.IB_VARCHAR,
                InnoDB.ib_col_attr_t.IB_COL_NONE, (short) 0, 32));
        showAssert(InnoDB.ib_table_schema_add_col(ib_tbl_sch.getValue(), "c3",
                InnoDB.ib_col_type_t.IB_INT,
                InnoDB.ib_col_attr_t.IB_COL_UNSIGNED, (short) 0, 4));

        PointerByReference ib_idx_sch = new PointerByReference();
        showAssert(InnoDB.ib_table_schema_add_index(ib_tbl_sch.getValue(),
                "c1_c2", ib_idx_sch));
        showAssert(InnoDB.ib_index_schema_add_col(ib_idx_sch.getValue(), "c1",
                0));
        showAssert(InnoDB.ib_index_schema_add_col(ib_idx_sch.getValue(), "c2",
                0));
        showAssert(InnoDB.ib_index_schema_set_clustered(ib_idx_sch.getValue()));

        Pointer trx = InnoDB
                .ib_trx_begin(InnoDB.ib_trx_level_t.IB_TRX_REPEATABLE_READ);
        showAssert(InnoDB.ib_schema_lock_exclusive(trx));

        LongBuffer buffer = LongBuffer.allocate(1);
        showAssert(InnoDB.ib_table_create(trx, ib_tbl_sch.getValue(), buffer));

        showAssert(InnoDB.ib_trx_commit(trx));
    }

    private void insertRows() {
        Pointer trx = InnoDB
                .ib_trx_begin(InnoDB.ib_trx_level_t.IB_TRX_REPEATABLE_READ);

        PointerByReference ib_crsr = new PointerByReference();

        showAssert(InnoDB.ib_cursor_open_table(TABLE_NAME, trx, ib_crsr));
        showAssert(InnoDB.ib_cursor_lock(ib_crsr.getValue(),
                InnoDB.ib_lck_mode_t.IB_LOCK_IX));

        Pointer tupl = InnoDB.ib_clust_read_tuple_create(ib_crsr.getValue());
        for (int i = 0; i < 5; i++) {
            System.out.println("ROW " + i);
            String key = "hello" + System.currentTimeMillis();
            Pointer c1 = getString(key);

            String value = ("world" + System.currentTimeMillis());
            Pointer c2 = getString(value);

            showAssert(InnoDB.ib_col_set_value(tupl, 0, c1, key.length() + 1));
            showAssert(InnoDB.ib_col_set_value(tupl, 1, c2, value.length() + 1));
            showAssert(InnoDB.ib_tuple_write_u32(tupl, 2, i));
            showAssert(InnoDB.ib_cursor_insert_row(ib_crsr.getValue(), tupl));
            InnoDB.ib_tuple_clear(tupl);
        }

        showAssert(InnoDB.ib_cursor_close(ib_crsr.getValue()));
        showAssert(InnoDB.ib_trx_commit(trx));
    }

    private Pointer getString(String string) {
        if (string.length() > 1023) {
            throw new IllegalArgumentException("string > 1023 characters");
        }

        Memory m = new Memory(1024);
        ByteBuffer buf = m.getByteBuffer(0, m.getSize()).order(
                ByteOrder.nativeOrder());
        buf.put(string.getBytes()).put((byte) 0).flip();

        return Native.getDirectBufferPointer(buf);
    }

    private void showAssert(int value) {
        if (InnoDB.db_err.DB_SUCCESS != value) {
            Exception e = new Exception();
            System.out.println(InnoDB.ib_strerror(value).getString(0));
            e.printStackTrace();
        }
    }
}
