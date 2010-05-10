package com.g414.inno.db;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class TableBuilder {
    private final String name;
    private final List<ColumnDef> columns;
    private final Map<String, List<IndexPart>> indexes;

    public TableBuilder(String name) {
        this.name = name;
        this.columns = new ArrayList<ColumnDef>();
        this.indexes = new LinkedHashMap<String, List<IndexPart>>();
    }

    public String getName() {
        return name;
    }

    public List<ColumnDef> getColumns() {
        return columns;
    }

    public TableBuilder addColumn(ColumnDef def) {
        this.columns.add(def);

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

    public Map<String, List<IndexPart>> getIndexes() {
        return indexes;
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
