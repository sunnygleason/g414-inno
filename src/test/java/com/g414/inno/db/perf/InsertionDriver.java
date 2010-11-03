package com.g414.inno.db.perf;

import static com.sun.faban.driver.CycleType.THINKTIME;
import static com.sun.faban.driver.Timing.MANUAL;
import static java.util.concurrent.TimeUnit.MICROSECONDS;

import java.util.Map;

import com.g414.inno.db.Transaction;
import com.g414.inno.db.TransactionLevel;
import com.g414.inno.db.tpl.TransactionCallback;
import com.google.inject.Key;
import com.sun.faban.driver.BenchmarkDefinition;
import com.sun.faban.driver.BenchmarkDriver;
import com.sun.faban.driver.BenchmarkOperation;
import com.sun.faban.driver.DriverContext;
import com.sun.faban.driver.FlatMix;
import com.sun.faban.driver.NegativeExponential;

@BenchmarkDefinition(name = "InsertionDriver", version = "1.0")
@BenchmarkDriver(name = "InsertionDriver", responseTimeUnit = MICROSECONDS)
@FlatMix(operations = { "Insert" }, mix = { 1.0 }, deviation = 1.0)
public class InsertionDriver extends PerfDriverBase {
    @BenchmarkOperation(name = "Insert", max90th = 1000000, timing = MANUAL)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void doInsert() throws Exception {
        String nextId = keyRange.next().toString();
        final Map<String, Object> nextEntity = entityGen.getEntity(nextId);

        DriverContext.getContext().recordTime();

        try {
            template.inTransaction(TransactionLevel.REPEATABLE_READ,
                    new TransactionCallback<Boolean>() {
                        @Override
                        public Boolean inTransaction(Transaction txn) {
                            return template.insert(txn, tableDef, nextEntity);
                        }
                    });
        } finally {
            DriverContext.getContext().recordTime();
        }
    }

    public static class GuiceModule extends PerfModuleBase {
        public GuiceModule() {
            super(true, 0L);
        }

        @Override
        protected void configure() {
            super.configure();

            bind(Key.get(Object.class, BenchmarkDriver.class)).to(
                    InsertionDriver.class);
        }
    }
}
