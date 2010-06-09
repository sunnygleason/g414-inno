package com.g414.inno.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class TableBuilder {
    private final String name;
    private final Map<String, ColumnDef> columns;
    private final Map<String, List<IndexPart>> indexes;
    private volatile AtomicInteger index = new AtomicInteger(); 
    
    public TableBuilder(String name) {
        this.name = name;
        this.columns = new LinkedHashMap<String, ColumnDef>();
        this.indexes = new LinkedHashMap<String, List<IndexPart>>();
    }

    public TableBuilder addColumn(String name, ColumnType type, int length,
            ColumnAttribute... attrs) {
        ColumnDef def = new ColumnDef(index.getAndIncrement(), name, type,
                length, attrs);
        this.columns.put(def.getName(), def);

        return this;
    }

    public TableBuilder addIndex(String indexName, String column,
            int prefixLen, boolean clustered, boolean unique) {
        if (!this.indexes.containsKey(indexName)) {
            this.indexes.put(indexName, new ArrayList<IndexPart>());
        }
        this.indexes.get(indexName).add(
                new IndexPart(column, prefixLen, clustered, unique));

        return this;
    }

    public TableDef build() {
        Map<String, IndexDef> defs = createIndexDefMap();

        return new TableDef(name, columns, defs);
    }

    private Map<String, IndexDef> createIndexDefMap() {
        Map<String, IndexDef> defs = new LinkedHashMap<String, IndexDef>();
        for (Map.Entry<String, List<IndexPart>> entry : indexes.entrySet()) {
            boolean clustered = false;
            boolean unique = false;
            for (IndexPart part : entry.getValue()) {
                clustered |= part.isClustered();
                unique |= part.isUnique();
            }
            IndexDef idx = new IndexDef(entry.getKey(), entry.getValue(),
                    clustered, unique);

            defs.put(entry.getKey(), idx);
        }

        return defs;
    }

    public static class IndexPart {
        private final String column;
        private final int prefixLen;
        private final boolean clustered;
        private final boolean unique;

        public IndexPart(String column, int prefixLen, boolean clustered,
                boolean unique) {
            this.column = column;
            this.prefixLen = prefixLen;
            this.clustered = clustered;
            this.unique = unique;
        }

        public String getColumn() {
            return column;
        }

        public int getPrefixLen() {
            return prefixLen;
        }

        public boolean isClustered() {
            return clustered;
        }

        public boolean isUnique() {
            return unique;
        }
    }
}
