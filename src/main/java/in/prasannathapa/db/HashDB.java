package in.prasannathapa.db;

import in.prasannathapa.db.data.FixedRecord;

import javax.naming.SizeLimitExceededException;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HashDB<K extends FixedRecord, V extends FixedRecord> {
    public static final String DB_DIR = System.getProperty("user.home") + File.separator + "HashDB"; //NO I18N
    public static final int cores = Runtime.getRuntime().availableProcessors();
    private static final Map<String, HashDB<FixedRecord,FixedRecord>> instanceMap = new Hashtable<>();

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
        HashDB<FixedRecord, FixedRecord> existingDb = instanceMap.get(dbName);
        if (existingDb != null) {
            existingDb.close();
        }
        HashDB<K, V> db = new HashDB<>(keyLength, valueLength, size, dbName);
        instanceMap.put(dbName, (HashDB<FixedRecord, FixedRecord>) db);
        return db;
    }
    public synchronized static void closeDB(String dbName) throws IOException {
        HashDB<?,?> existingDb = instanceMap.get(dbName);
        if (existingDb != null) {
            existingDb.close();
        }
        instanceMap.remove(dbName);
    }
    public static <K extends FixedRecord, V extends FixedRecord> HashDB<K, V> readFrom(String dbName) throws IOException {
        HashDB<FixedRecord,FixedRecord>  db = instanceMap.get(dbName);
        if (db == null) {
            db = new HashDB<>(dbName);
            instanceMap.put(dbName, db);
        }

        return (HashDB<K, V>) db;
    }
    public static void deleteDB(String dbName) throws IOException {
        HashDB db = instanceMap.get(dbName);
        if (db != null) {
            db.close();
            instanceMap.remove(dbName);
        }
        DBUtil.delete(dbName);
    }
    public void put(K key, V data) throws SizeLimitExceededException {
        writer.put(key, data);
    }

    public FixedRecord remove(K key) {
        return writer.remove(key);
    }

    private void close() throws IOException {
        util.close();
        metaData.close();
        writer.close();
        for (DBReader<K> reader : readers) {
            reader.close();
        }
    }

    public FixedRecord get(K key) {
        return readers[selector.getAndUpdate(i -> (i + 1) % cores)].get(key);
    }
    public FixedRecord[] getAll(K[] keys) {
        FixedRecord[] records = new FixedRecord[keys.length];
        DBReader<K> reader = readers[selector.getAndUpdate(i -> (i + 1) % cores)];
        for(int i = 0; i < keys.length; i++) {
            records[i] = reader.get(keys[i]);
        }
        return records;
    }

    public FixedRecord[] removeAll(K[] keys) {
        FixedRecord[] records = new FixedRecord[keys.length];
        DBReader<K> reader = readers[selector.getAndUpdate(i -> (i + 1) % cores)];
        for(int i = 0; i < keys.length; i++) {
            records[i] = reader.get(keys[i]);
        }
        return records;
    }

    public void putAll(K[] keys, V[] values) throws SizeLimitExceededException {
        assert keys.length == values.length;
        for(int i = 0; i < keys.length; i++) {
            writer.put(keys[i],values[i]);
        }
    }
}
