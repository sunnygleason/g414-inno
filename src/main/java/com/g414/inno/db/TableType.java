package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

public enum TableType {
    REDUNDANT(InnoDB.ib_tbl_fmt_t.IB_TBL_REDUNDANT), COMPACT(
            InnoDB.ib_tbl_fmt_t.IB_TBL_COMPACT), DYNAMIC(
            InnoDB.ib_tbl_fmt_t.IB_TBL_DYNAMIC), COMPRESSED(
            InnoDB.ib_tbl_fmt_t.IB_TBL_COMPRESSED);

    private final int code;

    private TableType(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static TableType fromCode(int code) {
        return TableType.values()[code];
    }
}
