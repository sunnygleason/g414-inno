/**
 * 
 */
package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

public enum TransactionLevel {
    READ_UNCOMMITTED(InnoDB.ib_trx_level_t.IB_TRX_READ_UNCOMMITTED), READ_COMMITTED(
            InnoDB.ib_trx_level_t.IB_TRX_READ_COMMITTED), REPEATABLE_READ(
            InnoDB.ib_trx_level_t.IB_TRX_REPEATABLE_READ), SERIALIZABLE(
            InnoDB.ib_trx_level_t.IB_TRX_SERIALIZABLE);

    private final int code;

    private TransactionLevel(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TransactionLevel fromCode(int code) {
        return TransactionLevel.values()[code];
    }
}