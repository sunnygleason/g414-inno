/**
 * 
 */
package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

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