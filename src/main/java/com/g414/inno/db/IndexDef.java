package com.g414.inno.db;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.g414.inno.db.TableBuilder.IndexPart;

public class IndexDef {
    private final String name;
    private final boolean clustered;
    private final boolean unique;
    private final List<IndexPart> columns;

    public IndexDef(String name, List<IndexPart> columns, boolean clustered,
            boolean unique) {
        this.name = name;
        List<IndexPart> newColumns = new ArrayList<IndexPart>();
        newColumns.addAll(columns);
        this.columns = Collections.unmodifiableList(newColumns);
        this.clustered = clustered;
        this.unique = unique;
    }

    public String getName() {
        return name;
    }

    public List<IndexPart> getColumns() {
        return columns;
    }

    public boolean isClustered() {
        return clustered;
    }

    public boolean isUnique() {
        return unique;
    }
}
