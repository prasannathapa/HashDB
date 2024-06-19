package in.prasannathapa.db;

import in.prasannathapa.db.data.Key;
import in.prasannathapa.db.data.Value;

import javax.naming.SizeLimitExceededException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class HashDB implements AutoCloseable {
    public static final String DB_DIR = System.getProperty("user.home") + File.separator + "HashDB"; //NO I18N
    public static final int cores = Runtime.getRuntime().availableProcessors();
    private static final Map<String, HashDB> instanceMap = new Hashtable<>();

    static {
        new File(DB_DIR).mkdirs();
    }

    public final String dbName;
    private final DBWriter writer;
    private final DBReader[] readers = new DBReader[cores];
    private final DBUtil meta;
    private final AtomicInteger selector = new AtomicInteger(0);

    private HashDB(int keyLength, int valueLength, int size, float loadFactor, String dbName) throws IOException, SizeLimitExceededException {
        this.dbName = dbName;
        this.meta = new DBUtil(keyLength, valueLength, loadFactor, size, dbName);
        this.writer = new DBWriter(keyLength, valueLength, size, loadFactor, meta);
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new DBReader(meta, writer.metaData);
        }
    }

    private HashDB(String dbName) throws IOException {
        this.dbName = dbName;
        this.meta = new DBUtil(dbName);
        this.writer = new DBWriter(meta);
        for (int i = 0; i < readers.length; i++) {
            readers[i] = new DBReader(meta, writer.metaData);
        }
    }

    //Creates a new Instance, deletes the old one if already existing
    public synchronized static HashDB createDB(int keyLength, int valueLength, int size, float loadFactor, String dbName) throws SizeLimitExceededException, IOException {
        HashDB db = instanceMap.get(dbName);
        if (db != null) {
            db.delete();
        }
        db = new HashDB(keyLength, valueLength, size, loadFactor, dbName);
        instanceMap.put(dbName, db);
        return db;
    }

    public static HashDB readFrom(String dbName) throws IOException {
        HashDB db = instanceMap.get(dbName);
        if (db != null) {
            return db;
        }
        return instanceMap.put(dbName, new HashDB(dbName));
    }

    public void put(Key key, Value value) throws SizeLimitExceededException, InvalidKeyException {
        writer.put(key, value);
    }

    public void remove(Key key) throws InvalidKeyException {
        writer.remove(key);
    }

    @Override
    public void close() throws IOException {
        meta.close();
        writer.close();
        for (DBReader reader : readers) {
            reader.close();
        }
    }

    private void delete() throws IOException {
        close();
    }

    public ByteBuffer get(Key key) throws InvalidKeyException {
        return readers[selector.getAndUpdate(i -> (i + 1) % cores)].get(key);
    }
}
