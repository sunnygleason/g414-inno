package com.g414.inno.db.tpl;

import com.g414.inno.db.Transaction;

public interface TransactionCallback<T> {
    public T inTransaction(Transaction txn);
}
