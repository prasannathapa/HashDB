package benchmark;

import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;
import in.prasannathapa.db.remote.RemoteHashDB;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import utils.SequenceGenerator;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 10, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
public class RemoteBenchmark {
    @Param({"4", "6", "16", "32"})
    private int keySize;

    @Param({"2", "10", "25", "50", "100"})
    private int dataSize;
    public static String host = "prasanna-14854-t";
    public static int port = 1090;
    RemoteHashDB client;
    private SequenceGenerator putSeq, getSeq, remSeq;
    private String dbName;

    private static final int BATCH_SIZE = 200;
    @Setup(Level.Trial)
    public void setUp() throws IOException, NotBoundException, SizeLimitExceededException {
        dbName = "hashDB_" + keySize + "_" + dataSize;
        System.out.println("Setup for");
        System.out.println("KeySize: " + keySize + " DataSize: " + dataSize);
        client = (RemoteHashDB) LocateRegistry.getRegistry(host, port).lookup(HashDB.class.getName());
        putSeq = new SequenceGenerator(0, keySize);
        getSeq = new SequenceGenerator(0, keySize);
        remSeq = new SequenceGenerator(1, keySize);
        client.createDB(dbName,keySize,dataSize,15_000_000);
        FixedRecord[] keys = new FixedRecord[BATCH_SIZE];
        FixedRecord[] values = new FixedRecord[BATCH_SIZE];
        for(int b = 0; b < 15_000_000/BATCH_SIZE; b++){
            for(int i = 0; i < BATCH_SIZE; i++){
                keys[i] = new Data(putSeq.getNextKey());
                values[i] = new Data(dataSize);
            }
            client.putAll(dbName,keys,values);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        client.deleteDB(dbName);
        System.out.println("Teardown: "+dbName);
    }


    @Threads(16)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkGet(Blackhole bh) throws IOException {
        FixedRecord[] keys = new FixedRecord[BATCH_SIZE];
        for(int i = 0; i < BATCH_SIZE; i++){
            keys[i] = new Data(getSeq.getNextKey());
        }
        bh.consume(client.getAll(dbName,keys));
    }

    @Threads(16)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkRemove(Blackhole bh) throws IOException {
        FixedRecord[] keys = new FixedRecord[BATCH_SIZE];
        for(int i = 0; i < BATCH_SIZE; i++){
            keys[i] = new Data(remSeq.getNextKey());
        }
        client.removeAll(dbName,keys);
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder().include(RemoteBenchmark.class.getSimpleName()).forks(1).build();
        new Runner(opt).run();
    }
}
