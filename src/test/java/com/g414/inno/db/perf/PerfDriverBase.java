package com.g414.inno.db.perf;

import java.util.Iterator;

import com.g414.dgen.EntityGenerator;
import com.g414.inno.db.TableDef;
import com.g414.inno.db.tpl.DatabaseTemplate;
import com.google.inject.Inject;

public class PerfDriverBase {
    @Inject
    protected TableDef tableDef;

    @Inject
    protected DatabaseTemplate template;

    @Inject
    protected Iterator<Long> keyRange;

    @Inject
    protected EntityGenerator entityGen;
}
