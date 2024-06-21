package in.prasannathapa.db;

import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;

import javax.naming.SizeLimitExceededException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HashDB<K extends FixedRecord, V extends FixedRecord> implements AutoCloseable {
    public static final String DB_DIR = System.getProperty("user.home") + File.separator + "HashDB"; //NO I18N
    public static final int cores = Runtime.getRuntime().availableProcessors();
    private static final Map<String, HashDB<?,?>> instanceMap = new Hashtable<>();

    static {
        new File(DB_DIR).mkdirs();
    }

    public final String dbName;
    private final DBWriter<K,V> writer;
    private final MetaData metaData;
    private final DBReader<K>[] readers = new DBReader[cores];
    private final DBUtil util;
    private final AtomicInteger selector = new AtomicInteger(0);

    private HashDB(int keyLength, int valueLength, int maxSize, String dbName) throws IOException, SizeLimitExceededException {
        this.dbName = dbName;
        this.util = new DBUtil(keyLength, valueLength, maxSize, dbName);
        this.metaData = new MetaData(keyLength,valueLength,maxSize, util.getBuffer(Resource.META, FileChannel.MapMode.READ_WRITE));
        this.writer = new DBWriter<>(metaData, util, true);
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new DBReader<>(util, writer.metaData);
        }
    }

    private HashDB(String dbName) throws IOException {
        this.dbName = dbName;
        this.util = new DBUtil(dbName);
        this.metaData = new MetaData(util.getBuffer(Resource.META, FileChannel.MapMode.READ_WRITE));
        this.writer = new DBWriter<>(metaData,util);
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new DBReader<>(util, writer.metaData);
        }
    }

    //Creates a new Instance, deletes the old one if already existing
    public synchronized static <K extends FixedRecord, V extends FixedRecord> HashDB<K, V> createDB(int keyLength, int valueLength, int size, String dbName) throws SizeLimitExceededException, IOException {
        HashDB<?, ?> existingDb = instanceMap.get(dbName);
        if (existingDb != null) {
            existingDb.close();
        }
        HashDB<K, V> db = new HashDB<>(keyLength, valueLength, size, dbName);
        instanceMap.put(dbName, db);
        return db;
    }

    public static HashDB<?,?> readFrom(String dbName) throws IOException {
        HashDB<?,?>  db = instanceMap.get(dbName);
        if (db != null) {
            return db;
        }
        return instanceMap.put(dbName, new HashDB<>(dbName));
    }
    public static void delete(String dbName) throws IOException {
        instanceMap.remove(dbName);
        DBUtil.delete(dbName);
    }
    public void put(K key, V data) throws SizeLimitExceededException {
        writer.put(key, data);
    }

    public void remove(K key) {
        writer.remove(key);
    }

    @Override
    public void close() throws IOException {
        util.close();
        metaData.close();
        writer.close();
        for (DBReader<K> reader : readers) {
            reader.close();
        }
    }

    public void delete() throws IOException {
        util.delete();
    }

    public Data get(K key) {
        return readers[selector.getAndUpdate(i -> (i + 1) % cores)].get(key);
    }
}
