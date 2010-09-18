package com.g414.inno.db.perf;

import java.util.Iterator;
import java.util.Random;

import com.g414.dgen.EntityGenerator;
import com.g414.dgen.range.LongRangeBuilder;
import com.google.inject.TypeLiteral;

public abstract class PerfModuleBase extends DatabaseModule {
    protected final long seed;
    
    public PerfModuleBase(boolean doTruncate, long seed) {
        super(doTruncate);
        this.seed = seed;
    }

    @Override
    protected void configure() {
        super.configure();

        int len = Integer.parseInt(System.getProperty("len", "64"));
        long min = Long.parseLong(System.getProperty("min", "1"));
        long max = Long.parseLong(System.getProperty("max", "1000000"));
        boolean isRandom = Boolean.parseBoolean(System.getProperty("random",
                "false"));

        Random theRandom = isRandom ? EntityGeneratorProvider.getRandom(seed)
                : null;

        bind(new TypeLiteral<Iterator<Long>>() {
        }).toInstance(
                (new LongRangeBuilder(min, max)).withRandom(theRandom)
                        .setRepeating(false).build().iterator());

        bind(EntityGenerator.class).toProvider(
                new EntityGeneratorProvider(len, seed));
    }
}
