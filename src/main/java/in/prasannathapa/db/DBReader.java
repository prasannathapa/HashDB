package in.prasannathapa.db;

import in.prasannathapa.db.data.BucketNode;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class DBReader<K extends FixedRecord> implements AutoCloseable {
    private final MetaData metaData;
    private MappedByteBuffer indexReader;
    private MappedByteBuffer collisionReader;
    private MappedByteBuffer dataReader;
    public DBReader(DBUtil dbUtil, MetaData metaData) throws IOException {
        this.metaData = metaData;
        indexReader = dbUtil.getBuffer(Resource.INDEX, FileChannel.MapMode.READ_ONLY);
        collisionReader = dbUtil.getBuffer(Resource.COLLISION, FileChannel.MapMode.READ_ONLY);
        dataReader = dbUtil.getBuffer(Resource.DATA, FileChannel.MapMode.READ_ONLY);

    }

    public synchronized FixedRecord get(K key) {
        Data dbKey = new Data(metaData.getKeySize());
        int bucket = metaData.getBucket(key);
        int bucketPointer = bucket * Integer.BYTES;
        indexReader.position(bucketPointer);
        int pos = indexReader.getInt();
        if(pos == BucketNode.NOT_WRITTEN){
            return null;
        } else if(pos >= 0) {
            dbKey.read(dataReader,pos);
            if(key.matches(dbKey)){
                FixedRecord value = new Data(metaData.getValueSize());
                value.read(dataReader);
                return value;
            }
        } else {
            pos = (pos == Integer.MIN_VALUE ? 0 : -pos);
            BucketNode record = new BucketNode(metaData.getKeySize(),pos,dataReader,collisionReader);
            while (!record.matches(key) && record.hasNext()) {
                record.readNext();
            }
            if(record.matches(key)){
                FixedRecord value = new Data(metaData.getValueSize());
                value.read(dataReader,record.dataPointer + metaData.getKeySize());
                return value;
            }
        }
        return null;
    }
    @Override
    public void close() {
        DBUtil.closeBuffer(dataReader);
        DBUtil.closeBuffer(collisionReader);
        DBUtil.closeBuffer(indexReader);
        indexReader = null;
        collisionReader = null;
        dataReader = null;
    }
}
