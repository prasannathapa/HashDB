package benchmark;


import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import utils.SequenceGenerator;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
public class LocalBenchmark {
    @Param({"4", "8", "16", "32"})
    private int keySize;

    @Param({"2", "5", "10", "25", "50", "100"})
    private int dataSize;

    private HashDB<FixedRecord, FixedRecord> db;
    private SequenceGenerator putSeq, getSeq, remSeq;
    private String dbName;
    @Param({"5000000"})
    private int entries;

    @Setup(Level.Trial)
    public void setUp() throws SizeLimitExceededException, IOException {
        dbName = "HashDB_" + keySize + "_" + dataSize;
        db = HashDB.createDB(keySize, dataSize, entries, dbName);
        putSeq = new SequenceGenerator(0, keySize);
        getSeq = new SequenceGenerator(0, keySize);
        remSeq = new SequenceGenerator(1, keySize);
        for(int i = 0; i < entries; i++){
            db.put(new Data(putSeq.getNextKey()), new Data(dataSize));
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        HashDB.deleteDB(dbName);
        System.out.println("Teardown: "+dbName);
    }


    @Threads(16)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkGet(Blackhole bh) {
        bh.consume(db.get(new Data(getSeq.getNextKey())));
    }

    @Threads(16)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkRemove(Blackhole bh) {
        bh.consume(db.remove(new Data(remSeq.getNextKey())));
    }
}
