package com.g414.inno.db.tpl;

import java.util.List;
import java.util.Map;

import com.g414.inno.db.ColumnDef;
import com.g414.inno.db.Cursor;
import com.g414.inno.db.Database;
import com.g414.inno.db.IndexDef;
import com.g414.inno.db.LockMode;
import com.g414.inno.db.SearchMode;
import com.g414.inno.db.TableDef;
import com.g414.inno.db.Transaction;
import com.g414.inno.db.TransactionLevel;
import com.g414.inno.db.Tuple;
import com.g414.inno.db.TupleBuilder;
import com.g414.inno.db.TupleBuilder.Options;

public class DatabaseTemplate {
    protected final Database database;

    public DatabaseTemplate(Database database) {
        this.database = database;
    }

    public <T> T inTransaction(TransactionLevel level,
            TransactionCallback<T> callback) throws Exception {
        Transaction txn = database.beginTransaction(level);
        try {
            return callback.inTransaction(txn);
        } catch (Exception e) {
            txn.rollback();
            txn = null;

            throw e;
        } finally {
            if (txn != null) {
                txn.commit();
            }
        }
    }

    public Map<String, Object> load(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toReturn = null;
        try {
            c = txn.openTable(def);

            TupleBuilder tpl = createTupleBuilder(def, primary.getColumns(),
                    data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toReturn = c.createClusteredIndexReadTuple();
                c.readRow(toReturn);

                Map<String, Object> found = toReturn.valueMap();

                if (!matchesPrimaryKey(primary, data, found)) {
                    return null;
                }
            } else {
                return null;
            }

            Map<String, Object> res = toReturn.valueMap();
            toReturn.clear();

            return res;
        } finally {
            if (toReturn != null) {
                toReturn.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean insert(Transaction txn, TableDef def,
            Map<String, Object> data) {
        Cursor c = null;
        Tuple toInsert = null;
        try {
            c = txn.openTable(def);

            toInsert = c.createClusteredIndexReadTuple();
            TupleBuilder tpl = createTupleBuilder(def, data);

            return c.insertRow(toInsert, tpl);
        } finally {
            if (toInsert != null) {
                toInsert.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean update(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toUpdate = null;
        try {
            c = txn.openTable(def);

            TupleBuilder tpl = createTupleBuilder(def, primary.getColumns(),
                    data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toUpdate = c.createClusteredIndexReadTuple();
                c.readRow(toUpdate);

                Map<String, Object> found = toUpdate.valueMap();
                if (!matchesPrimaryKey(primary, data, found)) {
                    return false;
                }
            } else {
                return false;
            }

            TupleBuilder val = createTupleBuilder(def, data);

            return c.updateRow(toUpdate, val);
        } finally {
            if (toUpdate != null) {
                toUpdate.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean insertOrUpdate(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toInsert = null;
        Tuple toUpdate = null;

        try {
            c = txn.openTable(def);

            TupleBuilder val = createTupleBuilder(def, data);
            TupleBuilder tpl = createTupleBuilder(def, primary.getColumns(),
                    data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toUpdate = c.createClusteredIndexReadTuple();
                c.readRow(toUpdate);

                Map<String, Object> found = toUpdate.valueMap();

                if (matchesPrimaryKey(primary, data, found)) {
                    c.updateRow(toUpdate, val);

                    return true;
                }
            }

            toInsert = c.createClusteredIndexReadTuple();
            c.insertRow(toInsert, val);

            return false;
        } finally {
            if (toInsert != null) {
                toInsert.delete();
            }

            if (toUpdate != null) {
                toUpdate.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    public boolean delete(Transaction txn, TableDef def,
            Map<String, Object> data) {
        IndexDef primary = def.getPrimaryIndex();
        Cursor c = null;
        Tuple toFind = null;
        Tuple toDelete = null;
        try {
            c = txn.openTable(def);

            TupleBuilder tpl = createTupleBuilder(def, primary.getColumns(),
                    data);

            toFind = c.createClusteredIndexSearchTuple(tpl);
            c.find(toFind, SearchMode.GE);

            if (c.isPositioned() && c.hasNext()) {
                toDelete = c.createClusteredIndexReadTuple();
                c.readRow(toDelete);

                Map<String, Object> found = toDelete.valueMap();
                if (matchesPrimaryKey(primary, data, found)) {
                    c.deleteRow();
                    toDelete.clear();

                    return true;
                }
            }

            return false;
        } finally {
            if (toDelete != null) {
                toDelete.delete();
            }

            if (toFind != null) {
                toFind.delete();
            }

            if (c != null) {
                c.close();
            }
        }
    }

    private static boolean matchesPrimaryKey(IndexDef primary,
            Map<String, Object> toFind, Map<String, Object> found) {
        for (ColumnDef col : primary.getColumns()) {
            String colName = col.getName();
            Object seekVal = toFind.get(colName);
            Object foundVal = found.get(colName);

            if ((seekVal == null && foundVal != null)
                    || (seekVal != null && !seekVal.equals(foundVal.toString()))) {
                return false;
            }
        }
        return true;
    }

    private static TupleBuilder createTupleBuilder(TableDef table,
            Map<String, Object> data) {
        TupleBuilder tpl = new TupleBuilder(table, Options.COERCE);
        for (ColumnDef col : table.getColDefs()) {
            tpl.addValue(data.get(col.getName()));
        }

        return tpl;
    }

    private static TupleBuilder createTupleBuilder(TableDef table,
            List<ColumnDef> defs, Map<String, Object> data) {
        TupleBuilder tpl = new TupleBuilder(table, Options.COERCE);
        for (ColumnDef col : defs) {
            tpl.addValue(data.get(col.getName()));
        }

        return tpl;
    }
}
