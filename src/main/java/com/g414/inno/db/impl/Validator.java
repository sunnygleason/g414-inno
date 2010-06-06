package com.g414.inno.db.impl;

import com.g414.inno.db.ColumnDef;

public interface Validator<T> {
    public boolean isValid(T target, ColumnDef columnDef);
}
