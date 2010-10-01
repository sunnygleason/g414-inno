package com.g414.inno.db.perf;

import static com.g414.inno.db.ColumnType.BINARY;

import com.g414.inno.db.ColumnType;
import com.g414.inno.db.Database;
import com.g414.inno.db.TableBuilder;
import com.g414.inno.db.TableDef;
import com.g414.inno.db.tpl.DatabaseTemplate;
import com.google.inject.AbstractModule;

public class DatabaseModule extends AbstractModule {
    private final boolean doTruncate;

    public DatabaseModule(boolean doTruncate) {
        this.doTruncate = doTruncate;
    }

    @Override
    protected void configure() {
        int len = Integer.parseInt(System.getProperty("len", "64"));

        final Database database = new Database();
        database.createDatabase("inno");

        bind(Database.class).toInstance(database);
        bind(DatabaseTemplate.class).toInstance(new DatabaseTemplate(database));

        TableDef tableDef = (new TableBuilder("inno/bench")).addColumn("a",
                ColumnType.INT, 4).addColumn("b", BINARY, len).addIndex("P",
                "a", 0, true, true).build();
        bind(TableDef.class).toInstance(tableDef);

        if (!database.tableExists(tableDef)) {
            database.createTable(tableDef);
        } else if (doTruncate) {
            database.truncateTable(tableDef);
        }
    }
}
