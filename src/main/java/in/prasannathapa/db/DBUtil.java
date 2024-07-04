package in.prasannathapa.db;

import javax.naming.SizeLimitExceededException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class DBUtil {
    public final String dbName;

    private final RandomAccessFile[] resourceFiles = new RandomAccessFile[Resource.values().length] ;

    private final long[] diskSize = new long[resourceFiles.length];
    public final FileChannel[] channels = new FileChannel[resourceFiles.length];
    public DBUtil(String dbName) throws IOException {
        this.dbName = dbName;
        for (Resource resource : Resource.values()) {
            String path = String.join(File.separator, HashDB.DB_DIR, dbName, resource.name());
            if (Files.notExists(Path.of(path))) {
                throw new IOException("DB Does not Exist");
            }
            resourceFiles[resource.ordinal()] = new RandomAccessFile(path, "rw");
        }
        //Pre initialise Meta
        int metaIdx = Resource.META.ordinal();
        diskSize[metaIdx] = MetaData.BYTES;
        resourceFiles[metaIdx].setLength(MetaData.BYTES);
        channels[metaIdx] = resourceFiles[metaIdx].getChannel();
        MappedByteBuffer metaData = channels[metaIdx].map(FileChannel.MapMode.PRIVATE,0, Integer.BYTES * 3);
        int keySize = metaData.getInt();
        int valueSize = metaData.getInt();
        int entrySize = metaData.getInt();
        closeBuffer(metaData);
        //initialise rest of the data using MetaData
        long recordSize = keySize + valueSize;
        long fileSize = recordSize * entrySize;
        int buckets = MetaData.getBucketSize(entrySize);
        int bucketSize = buckets * Integer.BYTES;
        int collisionRecordSize = Integer.BYTES * 2;
        long collisionMaxFileSize = Math.min(Integer.MAX_VALUE,(long)entrySize * collisionRecordSize); //Assuming everything collided
        diskSize[Resource.DATA.ordinal()] = (int) fileSize;
        diskSize[Resource.INDEX.ordinal()] = bucketSize;
        diskSize[Resource.COLLISION.ordinal()] = collisionMaxFileSize;
        diskSize[Resource.COLLISION_BUBBLE.ordinal()] = (long) Integer.BYTES * entrySize;
        diskSize[Resource.DATA_BUBBLE.ordinal()] = (long) Integer.BYTES * entrySize;
        for (Resource resource : Resource.values()) {
            if(resource != Resource.META) {
                resourceFiles[resource.ordinal()].setLength(diskSize[resource.ordinal()]);
                channels[resource.ordinal()] = resourceFiles[resource.ordinal()].getChannel();
            }
        }
    }
    public DBUtil(int keyLength, int valueLength, int entries, String dbName) throws IOException, SizeLimitExceededException {
        this.dbName = dbName;

        long recordSize = keyLength + valueLength;
        long fileSize = recordSize * entries;
        if(fileSize > Integer.MAX_VALUE){
            throw new SizeLimitExceededException("file size required to store "+entries+" entries of "+recordSize+" bytes (key+value) exceeds 2GB");
        }
        int buckets = MetaData.getBucketSize(entries);
        int bucketSize = buckets * Integer.BYTES;
        int collisionRecordSize = Integer.BYTES * 2;
        long collisionMaxFileSize = Math.min(Integer.MAX_VALUE,(long)entries * collisionRecordSize); //Assuming everything collided
        diskSize[Resource.DATA.ordinal()] = (int) fileSize;
        diskSize[Resource.INDEX.ordinal()] = bucketSize;
        diskSize[Resource.COLLISION.ordinal()] = collisionMaxFileSize;
        diskSize[Resource.COLLISION_BUBBLE.ordinal()] = (long) Integer.BYTES * entries;
        diskSize[Resource.DATA_BUBBLE.ordinal()] = (long) Integer.BYTES * entries;
        diskSize[Resource.META.ordinal()] = MetaData.BYTES;
        initializeChannels();
    }

    private void initializeChannels() throws IOException {
        for(Resource resource: Resource.values()){
            int index = resource.ordinal();
            File file = new File(String.join(File.separator, HashDB.DB_DIR,dbName,resource.name()));
            if(!file.exists()) {
                file.getParentFile().mkdirs();
                file.createNewFile();
            }
            resourceFiles[index] = new RandomAccessFile(file, "rw");
            resourceFiles[index].setLength(diskSize[index]);
            channels[index] = resourceFiles[index].getChannel();
        }
    }
    public synchronized MappedByteBuffer getBuffer(Resource resource, FileChannel.MapMode mode) throws IOException {
        return channels[resource.ordinal()].map(mode,0,diskSize[resource.ordinal()]);
    }

    public void close(MetaData metaData) throws IOException {
        for(Resource resource: Resource.values()){
            FileChannel c = channels[resource.ordinal()];
            RandomAccessFile r = resourceFiles[resource.ordinal()];
            if(c != null) {
                c.truncate(metaData.getEndPointer(resource));
                c.close();
            }
            if(r != null) {
                r.close();
            }
        }
    }
    private static Method invokeCleaner;
    private static Object unsafe;
    static {
        if(System.getProperty("os.name").toLowerCase().contains("windows")) {
            Class<?> unsafeClass = null;
            try {
                unsafeClass = Class.forName("sun.misc.Unsafe");
                Field unsafeField = unsafeClass.getDeclaredField("theUnsafe");
                unsafeField.setAccessible(true);
                unsafe = unsafeField.get(null);
                invokeCleaner = unsafeClass.getMethod("invokeCleaner", ByteBuffer.class);
            } catch (Exception ignored) {}
        }
    }
    public static void closeBuffer(MappedByteBuffer buffer){
        if(buffer == null) return;
        buffer.force();
        if(invokeCleaner != null){
            try {
                invokeCleaner.invoke(unsafe,buffer);
            } catch (IllegalAccessException | InvocationTargetException ignore) {}
        }
    }

    public void delete() throws IOException {
        delete(dbName);
    }
    public static void delete(String dbName) throws IOException {
        Path path = Paths.get(HashDB.DB_DIR + File.separator + dbName);
        Files.walk(path).map(Path::toFile).forEach(File::delete);
        Files.deleteIfExists(path);
    }
}
