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

@BenchmarkDefinition(name = "UpdatingDriver", version = "1.0")
@BenchmarkDriver(name = "UpdatingDriver", responseTimeUnit = MICROSECONDS)
@FlatMix(operations = { "Update" }, mix = { 1.0 }, deviation = 1.0)
public class UpdatingDriver extends PerfDriverBase {
    @BenchmarkOperation(name = "Update", max90th = 1000000, timing = MANUAL)
    @NegativeExponential(cycleType = THINKTIME, cycleMean = 0, cycleDeviation = 0.0)
    public void doUpdate() throws Exception {
        String nextId = keyRange.next().toString();
        final Map<String, Object> nextEntity = entityGen.getEntity(nextId);

        DriverContext.getContext().recordTime();

        template.inTransaction(TransactionLevel.REPEATABLE_READ,
                new TransactionCallback<Boolean>() {
                    @Override
                    public Boolean inTransaction(Transaction txn) {
                        return template.update(txn, tableDef, nextEntity);
                    }
                });

        DriverContext.getContext().recordTime();
    }

    public static class GuiceModule extends PerfModuleBase {
        public GuiceModule() {
            super(false, 1L);
        }

        @Override
        protected void configure() {
            super.configure();

            bind(Key.get(Object.class, BenchmarkDriver.class)).to(
                    UpdatingDriver.class);
        }
    }
}
