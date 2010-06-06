package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

public enum Lock {
    INTENTION_SHARED(InnoDB.ib_lck_mode_t.IB_LOCK_IS), INTENTION_EXCLUSIVE(
            InnoDB.ib_lck_mode_t.IB_LOCK_IX), LOCK_SHARED(
            InnoDB.ib_lck_mode_t.IB_LOCK_S), LOCK_EXCLUSIVE(
            InnoDB.ib_lck_mode_t.IB_LOCK_X), NOT_USED(
            InnoDB.ib_lck_mode_t.IB_LOCK_NOT_USED), NONE(
            InnoDB.ib_lck_mode_t.IB_LOCK_NONE);

    private final int code;

    private Lock(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static Lock fromCode(int code) {
        return Lock.values()[code];
    }
}