package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Transaction {
    protected final Pointer trx;

    public Transaction(Pointer trx) {
        this.trx = trx;
    }

    public Cursor openTableByName(String table) {
        PointerByReference crsr = new PointerByReference();

        Util.assertSuccess(InnoDB.ib_cursor_open_table(table, trx, crsr));

        return new Cursor(crsr);
    }

    public Cursor openTableById(long id) {
        PointerByReference crsr = new PointerByReference();

        Util.assertSuccess(InnoDB.ib_cursor_open_table_using_id(id, trx, crsr));

        return new Cursor(crsr);
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

    public enum Level {
        READ_UNCOMMITTED(InnoDB.ib_trx_level_t.IB_TRX_READ_UNCOMMITTED), READ_COMMITTED(
                InnoDB.ib_trx_level_t.IB_TRX_READ_COMMITTED), REPEATABLE_READ(
                InnoDB.ib_trx_level_t.IB_TRX_REPEATABLE_READ), SERIALIZABLE(
                InnoDB.ib_trx_level_t.IB_TRX_SERIALIZABLE);

        private final int code;

        private Level(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static Level fromCode(int code) {
            return Level.values()[code];
        }
    }

    public enum State {
        NOT_STARTED(InnoDB.ib_trx_state_t.IB_TRX_NOT_STARTED), ACTIVE(
                InnoDB.ib_trx_state_t.IB_TRX_ACTIVE), COMMITTED_IN_MEMORY(
                InnoDB.ib_trx_state_t.IB_TRX_COMMITTED_IN_MEMORY), PREPARED(
                InnoDB.ib_trx_state_t.IB_TRX_PREPARED);

        private final int code;

        private State(int code) {
            this.code = code;
        }

        public int getCode() {
            return code;
        }

        public static State fromCode(int code) {
            return State.values()[code];
        }
    }
}
