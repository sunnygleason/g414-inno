package com.g414.inno.db;

import java.nio.LongBuffer;
import java.util.List;
import java.util.Map;

import com.g414.inno.db.TableBuilder.IndexPart;
import com.g414.inno.db.Transaction.Level;
import com.g414.inno.jna.impl.InnoDB;
import com.sun.jna.Pointer;
import com.sun.jna.ptr.PointerByReference;

public class Database {
    public Database() {
        Util.assertSuccess(InnoDB.ib_init());
        Util.assertSuccess(InnoDB.ib_startup("barracuda"));
    }

    public void createDatabase(String databaseName) {
        Util.assertSchemaOperationSuccess(InnoDB
                .ib_database_create(databaseName));
    }

    public void dropDatabase(String databaseName) {
        Util.assertSuccess(InnoDB.ib_database_drop(databaseName));
    }

    public Transaction beginTransaction(Transaction.Level level) {
        Pointer trx = InnoDB.ib_trx_begin(level.getCode());

        return new Transaction(trx);
    }

    public Schema createSchema(String name, TableType format, int pageSize) {
        PointerByReference schema = new PointerByReference();
        Transaction trx = this.beginTransaction(Level.REPEATABLE_READ);
        Util.assertSuccess(InnoDB.ib_schema_lock_exclusive(trx.getTrx()));
        Util.assertSuccess(InnoDB.ib_table_schema_create(name, schema, format
                .getCode(), pageSize));
        trx.commit();

        return new Schema(schema);
    }

    public Table createTable(Schema schema, TableBuilder builder) {
        boolean found = tableExists(builder.getName());

        if (found) {
            throw new InnoException("table already exists: "
                    + builder.getName());
        }

        for (ColumnDef def : builder.getColumns()) {
            int attr = 0;
            for (ColumnAttribute a : def.getAttrs()) {
                attr |= a.getCode();
            }

            Util.assertSuccess(InnoDB.ib_table_schema_add_col(schema
                    .getSchema().getValue(), def.getName(), def.getType()
                    .getCode(), attr, (short) 0, def.getLength().intValue()));
        }

        for (Map.Entry<String, List<IndexPart>> entry : builder.getIndexes()
                .entrySet()) {
            PointerByReference index = new PointerByReference();
            Util.assertSuccess(InnoDB.ib_table_schema_add_index(schema
                    .getSchema().getValue(), entry.getKey(), index));
            boolean clustered = false;
            boolean unique = false;

            for (IndexPart part : entry.getValue()) {
                clustered |= part.isClustered();
                unique |= part.isUnique();

                Util.assertSuccess(InnoDB.ib_index_schema_add_col(index
                        .getValue(), part.getColumn(), part.getPrefixLen()));
            }

            if (clustered) {
                Util.assertSuccess(InnoDB.ib_index_schema_set_clustered(index
                        .getValue()));
            } else if (unique) {
                Util.assertSuccess(InnoDB.ib_index_schema_set_unique(index
                        .getValue()));
            }
        }

        // TODO: cleanup locking and transactions w/ finally
        Transaction trx = this.beginTransaction(Level.REPEATABLE_READ);
        LongBuffer tableId = LongBuffer.allocate(1);

        try {
            Util.assertSuccess(InnoDB.ib_schema_lock_exclusive(trx.getTrx()));
            Util.assertSuccess(InnoDB.ib_table_create(trx.getTrx(), schema
                    .getSchema().getValue(), tableId));
            trx.commit();

            return new Table(builder.getName(), tableId.get(0));
        } catch (InnoException e) {
            trx.rollback();

            throw e;
        } finally {
            // TODO - better handling of rollback/release
        }
    }

    public Long truncateTable(String tableName) {
        LongBuffer tableId = LongBuffer.allocate(1);
        Util.assertSuccess(InnoDB.ib_table_truncate(tableName, tableId));

        return tableId.get();
    }

    public boolean tableExists(String name) {
        boolean found = false;
        Transaction check = null;
        try {
            check = this.beginTransaction(Level.REPEATABLE_READ);
            check.openTableByName(name);

            found = true;
        } catch (InnoException expected) {
            if (!expected.getMessage().contains("Table not found")) {
                throw expected;
            }
        } finally {
            if (check != null) {
                check.rollback();
            }
        }
        return found;
    }

    public void shutdown(boolean fast) {
        int flag = fast ? InnoDB.ib_shutdown_t.IB_SHUTDOWN_NO_BUFPOOL_FLUSH
                : InnoDB.ib_shutdown_t.IB_SHUTDOWN_NORMAL;
        Util.assertSuccess(InnoDB.ib_shutdown(flag));
    }
}
