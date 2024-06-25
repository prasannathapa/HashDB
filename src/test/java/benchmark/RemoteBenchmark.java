package benchmark;

import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;
import in.prasannathapa.db.remote.RemoteHashDB;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import utils.SequenceGenerator;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 5, time = 10)
public class RemoteBenchmark {
    @Param({"4", "88", "16", "32"})
    private int keySize;

    @Param({"2", "10", "25", "50", "100"})
    private int dataSize;

    @Param({"localhost"})
    private String host;
    @Param({"1099"})
    private int port;

    @Param({"200"})
    private int batchSize;
    @Param({"5000000"})
    private int entries;
    private RemoteHashDB client;

    private SequenceGenerator putSeq, getSeq, remSeq;
    private String dbName;

    @Setup(Level.Trial)
    public void setUp() throws IOException, SizeLimitExceededException, NotBoundException {
        if (client == null) {
            client = (RemoteHashDB) LocateRegistry.getRegistry(host, port).lookup(HashDB.class.getName());
        }
        dbName = "hashDB_" + keySize + "_" + dataSize;
        putSeq = new SequenceGenerator(0, keySize);
        getSeq = new SequenceGenerator(0, keySize);
        remSeq = new SequenceGenerator(1, keySize);
        client.createDB(dbName, keySize, dataSize, entries);
        int batch = Math.min(entries / batchSize, entries);
        FixedRecord[] keys = new FixedRecord[batch];
        FixedRecord[] values = new FixedRecord[batch];
        for (int b = 0; b < entries / batch; b++) {
            for (int i = 0; i < batch; i++) {
                keys[i] = new Data(putSeq.getNextKey());
                values[i] = new Data(dataSize);
            }
            client.putAll(dbName, keys, values);
        }
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        client.deleteDB(dbName);
    }


    @Threads(16)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkGet(Blackhole bh) throws IOException {
        FixedRecord[] keys = new FixedRecord[batchSize];
        for (int i = 0; i < batchSize; i++) {
            keys[i] = new Data(getSeq.getNextKey());
        }
        bh.consume(client.getAll(dbName, keys));
    }

    @Threads(16)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @OutputTimeUnit(TimeUnit.SECONDS)
    public void benchmarkRemove(Blackhole bh) throws IOException {
        FixedRecord[] keys = new FixedRecord[batchSize];
        for (int i = 0; i < batchSize; i++) {
            keys[i] = new Data(remSeq.getNextKey());
        }
        client.removeAll(dbName, keys);
    }
}
