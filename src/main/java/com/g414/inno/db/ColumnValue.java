package com.g414.inno.db;

public class ColumnValue {
    private final int colIndex;
    private final Object value;
    private final Integer length;

    public ColumnValue(int colIndex, Object value, Integer length) {
        this.colIndex = colIndex;
        this.value = value;
        this.length = length;
    }

    public int getColIndex() {
        return colIndex;
    }

    public Object getValue() {
        return value;
    }

    public Integer getLength() {
        return length;
    }
}
