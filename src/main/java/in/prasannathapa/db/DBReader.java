package in.prasannathapa.db;

import in.prasannathapa.db.data.CollisionRecord;
import in.prasannathapa.db.data.Key;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;

import static in.prasannathapa.db.DBWriter.NOT_WRITTEN;

class DBReader implements AutoCloseable {
    public final DBUtil dbUtil;
    private final MetaData metaData;
    private MappedByteBuffer indexReader;
    private MappedByteBuffer collisionReader;
    private MappedByteBuffer dataReader;
    public DBReader(DBUtil dbUtil, MetaData metaData) throws IOException {
        this.dbUtil = dbUtil;
        this.metaData = metaData;
        indexReader = dbUtil.getBuffer(Resource.INDEX, FileChannel.MapMode.READ_ONLY);
        collisionReader = dbUtil.getBuffer(Resource.COLLISION, FileChannel.MapMode.READ_ONLY);
        dataReader = dbUtil.getBuffer(Resource.DATA, FileChannel.MapMode.READ_ONLY);

    }

    public synchronized ByteBuffer get(Key key) throws InvalidKeyException {
        byte[] keyBytes = new byte[metaData.keySize];
        byte[] value = new byte[metaData.valueSize];
        if(key.size() != keyBytes.length) {
            throw new InvalidKeyException("key size is not equal to written bytes");
        }
        int bucket = key.hash() % metaData.buckets;
        int bucketPointer = bucket * Integer.BYTES;
        indexReader.position(bucketPointer);
        int pos = indexReader.getInt();
        if(pos == NOT_WRITTEN){
            return null;
        } else if(pos >= 0) {
            dataReader.position(pos);
            dataReader.get(keyBytes);
            if(key.matches(keyBytes)){
                dataReader.get(value);
                return ByteBuffer.wrap(value);
            }
        } else {
            pos = (pos == Integer.MIN_VALUE ? 0 : pos);
            CollisionRecord record = new CollisionRecord(key.size());
            collisionReader.position(-pos);
            record.read(collisionReader);
            while (!key.matches(record.key) && record.next != NOT_WRITTEN) {
                int nextPos = record.next;
                collisionReader.position(nextPos);
                record.read(collisionReader);
            }
            if(key.matches(record.key)){
                dataReader.position(record.pointer + key.size());
                dataReader.get(value);
                return ByteBuffer.wrap(value);
            }
        }
        return null;
    }
    @Override
    public void close() throws IOException {
        DBUtil.closeBuffer(dataReader);
        DBUtil.closeBuffer(collisionReader);
        DBUtil.closeBuffer(indexReader);
        indexReader = null;
        collisionReader = null;
        dataReader = null;
        dbUtil.close();
    }
}
