package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

public enum LockMode {
    /* see InnoDB.ib_lck_mode_t */
    INTENTION_SHARED(InnoDB.ib_lck_mode_t.IB_LOCK_IS), INTENTION_EXCLUSIVE(
            InnoDB.ib_lck_mode_t.IB_LOCK_IX), LOCK_SHARED(
            InnoDB.ib_lck_mode_t.IB_LOCK_S), LOCK_EXCLUSIVE(
            InnoDB.ib_lck_mode_t.IB_LOCK_X), NOT_USED(
            InnoDB.ib_lck_mode_t.IB_LOCK_NOT_USED), NONE(
            InnoDB.ib_lck_mode_t.IB_LOCK_NONE);

    private final int code;

    private LockMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static LockMode fromCode(int code) {
        return LockMode.values()[code];
    }
}