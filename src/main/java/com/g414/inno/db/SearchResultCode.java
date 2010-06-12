package com.g414.inno.db;

public enum SearchResultCode {
    /* returns -1, 0 or 1 based on search result */
    BEFORE(0), EQUALS(1), AFTER(2);

    private final int code;

    private SearchResultCode(int code) {
        this.code = code;
    }

    public int getCode() {
        return code - 1;
    }

    public static SearchResultCode fromCode(int code) {
        return SearchResultCode.values()[code + 1];
    }
}