package com.g414.inno.db;

import com.g414.inno.jna.impl.InnoDB;

public enum MatchMode {
    CLOSEST(InnoDB.ib_match_mode_t.IB_CLOSEST_MATCH), EXACT(
            InnoDB.ib_match_mode_t.IB_EXACT_MATCH), EXACT_PREFIX(
            InnoDB.ib_match_mode_t.IB_EXACT_PREFIX);

    private final int code;

    private MatchMode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code;
    }

    public static MatchMode fromCode(int code) {
        return MatchMode.values()[code];
    }
}