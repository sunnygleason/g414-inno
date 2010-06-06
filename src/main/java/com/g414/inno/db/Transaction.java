package com.g414.inno.db;

import com.g414.inno.db.impl.Util;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Transaction {
    protected final Pointer trx;

    public Transaction(Pointer trx) {
        this.trx = trx;
    }

    public Cursor openTable(TableDef tableDef) {
        PointerByReference crsr = new PointerByReference();

        Util.assertSuccess(InnoDB.ib_cursor_open_table(tableDef.getName(), trx,
                crsr));

        return new Cursor(crsr, tableDef);
    }

    public void commit() {
        Util.assertSuccess(InnoDB.ib_trx_commit(trx));
    }

    public void rollback() {
        Util.assertSuccess(InnoDB.ib_trx_rollback(trx));
    }

    public void release() {
        Util.assertSuccess(InnoDB.ib_trx_release(trx));
    }

    public void start(Level level) {
        Util.assertSuccess(InnoDB.ib_trx_start(trx, level.getCode()));
    }

    public State getState() {
        return State.fromCode(InnoDB.ib_trx_state(trx));
    }

    public Pointer getTrx() {
        return trx;
    }
}
