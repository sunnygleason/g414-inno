package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

public enum SearchMode {
    G(InnoDB.ib_srch_mode_t.IB_CUR_G), GE(InnoDB.ib_srch_mode_t.IB_CUR_GE), L(
            InnoDB.ib_srch_mode_t.IB_CUR_L), LE(InnoDB.ib_srch_mode_t.IB_CUR_LE);

    private final int code;

    private SearchMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static SearchMode fromCode(int code) {
        return SearchMode.values()[code - 1];
    }
}