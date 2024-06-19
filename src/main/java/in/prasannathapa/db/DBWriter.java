package in.prasannathapa.db;

import in.prasannathapa.db.utils.key.CollisionRecord;
import in.prasannathapa.db.data.Key;
import in.prasannathapa.db.data.Value;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.security.InvalidKeyException;

class DBWriter implements AutoCloseable{

    public static final int NOT_WRITTEN = -1;
    public final MetaData metaData;
    private final MappedByteBuffer[] buffers = new MappedByteBuffer[Resource.values().length];

    private DBWriter(MetaData metaData, DBUtil metaDB) throws IOException {
        this.metaData = metaData;
        for(Resource resource: Resource.values()){
            if(resource != Resource.META){
                this.buffers[resource.ordinal()] = metaDB.getBuffer(resource, FileChannel.MapMode.READ_WRITE);
            }
        }
    }
    public DBWriter(DBUtil metaDB) throws IOException {
        this(new MetaData(metaDB),metaDB);
    }
    public DBWriter(int keySize, int valueSize, int entries, float loadFactor, DBUtil metaDB) throws IOException {
        this(new MetaData(keySize, valueSize, entries, loadFactor, metaDB), metaDB);
        for (int i = 0; i < metaData.buckets; i++) {
            buffers[Resource.INDEX.ordinal()].putInt(NOT_WRITTEN);
        }
    }

    public synchronized void remove(Key key) throws InvalidKeyException {
        byte[] keyBytes = key.write();
        if(key.size() != keyBytes.length || metaData.keySize != key.size()) {
            throw new InvalidKeyException("key size is not equal to written bytes or metadata");
        }
        int bucket = key.hash() % metaData.buckets;
        int bucketPointer = bucket * Integer.BYTES;
        MappedByteBuffer idxBuffer = buffers[Resource.INDEX.ordinal()].position(bucketPointer);
        MappedByteBuffer dataBuffer = buffers[Resource.DATA.ordinal()];
        CollisionRecord record = new CollisionRecord(key.size());
        int pos = idxBuffer.getInt();
        if(pos == NOT_WRITTEN){
            return;
        } else if(pos >= 0) {
            dataBuffer.position(pos);
            record.read(dataBuffer);
            if(record.matches(keyBytes)) {
                idxBuffer.putInt(bucketPointer, NOT_WRITTEN);
                createBubble(Resource.DATA_BUBBLE, pos);
            }
        } else {
            pos = (pos == Integer.MIN_VALUE ? 0 : pos);
            int current = -pos;
            MappedByteBuffer collisionBuffer = buffers[Resource.COLLISION.ordinal()].position(current);
            record.read(collisionBuffer);
            // Handle removal of the head node
            if(record.matches(keyBytes)) {
                //if -1, write -1 else invert to neg pointer
                idxBuffer.putInt(bucketPointer,-Math.abs(record.next));
                createBubble(Resource.COLLISION_BUBBLE,current);
                return;
            }
            // Traverse the linked list to find and remove the node
            int prevRecordPos = current;
            while (record.next != NOT_WRITTEN) {
                int nextPos = record.next;
                collisionBuffer.position(nextPos);
                record.read(collisionBuffer);

                if (record.matches(keyBytes)) {
                    // Remove currentRecord from the linked list
                    collisionBuffer.position(prevRecordPos + key.size() + Integer.BYTES);
                    collisionBuffer.putInt(record.next);
                    createBubble(Resource.COLLISION_BUBBLE, record.position);
                    return;
                }
                prevRecordPos = nextPos;
            }
        }
    }
    public synchronized void put(Key key, Value value) throws InvalidKeyException, SizeLimitExceededException {
        byte[] keyBytes = key.write();
        byte[] valueBytes = value.write();
        if(key.size() != keyBytes.length || key.size() != metaData.keySize) {
            throw new InvalidKeyException("key size is not equal to written bytes");
        } else if (value.size() != valueBytes.length || value.size() != metaData.valueSize) {
            throw new InvalidKeyException("value size is not equal to written bytes");
        }
        int bubble;
        int bucket = key.hash() % metaData.buckets;
        int bucketPointer = bucket * Integer.BYTES;
        MappedByteBuffer idxBuffer = buffers[Resource.INDEX.ordinal()];
        MappedByteBuffer dataBuffer = buffers[Resource.DATA.ordinal()];
        MappedByteBuffer collisionBuffer = buffers[Resource.COLLISION.ordinal()];

        int pos = idxBuffer.getInt(bucketPointer);
        if(pos == NOT_WRITTEN){
            bubble = burstBubble(Resource.DATA_BUBBLE);
            if(bubble >= metaData.entries * (key.size() + value.size())){
                throw new SizeLimitExceededException("Max entries reached");
            }
            dataBuffer.position(bubble).put(keyBytes).put(valueBytes);
            idxBuffer.putInt(bucketPointer,bubble);
            if(metaData.getEndPointer(Resource.DATA) < dataBuffer.position()){
                metaData.setEndPointer(Resource.DATA, dataBuffer.position());
            }
        } else if (pos >= 0){
            byte[] existingKey = new byte[key.size()];
            dataBuffer.position(pos);
            dataBuffer.get(existingKey);
            if(key.matches(existingKey)){
                dataBuffer.put(valueBytes);
                //Its overwrite no need to update end pointer
            } else {
                bubble = burstBubble(Resource.DATA_BUBBLE);
                if(bubble >= metaData.entries * (key.size() + value.size())){
                    throw new SizeLimitExceededException("Max entries reached");
                }
                int colBubble = burstBubble(Resource.COLLISION_BUBBLE);
                idxBuffer.putInt(bucketPointer, colBubble == 0 ? Integer.MIN_VALUE : -colBubble);
                collisionBuffer.position(colBubble)
                        .put(existingKey).putInt(pos).putInt(NOT_WRITTEN);
                if (collisionBuffer.position() > metaData.getEndPointer(Resource.COLLISION)) {
                    metaData.setEndPointer(Resource.COLLISION, collisionBuffer.position());
                }
                int nextBubble = burstBubble(Resource.COLLISION_BUBBLE);
                collisionBuffer.position(colBubble + existingKey.length + Integer.BYTES)
                        .putInt(nextBubble) //Replace End Node
                        .position(nextBubble)
                        .put(keyBytes).putInt(bubble).putInt(NOT_WRITTEN);
                if (collisionBuffer.position() > metaData.getEndPointer(Resource.COLLISION)) {
                    metaData.setEndPointer(Resource.COLLISION, collisionBuffer.position());
                }
                dataBuffer.position(bubble).put(keyBytes).put(valueBytes);
                if(dataBuffer.position() > metaData.getEndPointer(Resource.DATA)){
                    metaData.setEndPointer(Resource.DATA,dataBuffer.position());
                }
            }
        } else {
            pos = (pos == Integer.MIN_VALUE ? 0 : pos);
            int head = -pos; //Negative denotes collision, -(-value) will make it positive
            CollisionRecord record = new CollisionRecord(key.size());
            record.read(collisionBuffer.position(head));
            while(record.next != NOT_WRITTEN && !record.matches(keyBytes)) {
                head = record.next;
                record.read(collisionBuffer.position(head));
            }
            //Key exists, update existing record
            if(record.matches(keyBytes)){
                dataBuffer.position(record.pointer + key.size());
                dataBuffer.put(valueBytes);
                //No need to update end pointer since its overwrite
            } else {
                //Insert new Record
                bubble = burstBubble(Resource.DATA_BUBBLE);
                if(bubble >= metaData.entries * (key.size() + value.size())){
                    throw new SizeLimitExceededException("Max entries reached");
                }
                int colBubble = burstBubble(Resource.COLLISION_BUBBLE);
                dataBuffer.position(bubble).put(keyBytes).put(valueBytes);
                collisionBuffer.position(colBubble)
                        .put(keyBytes).putInt(bubble).putInt(NOT_WRITTEN);
                if(collisionBuffer.position() > metaData.getEndPointer(Resource.COLLISION)){
                    metaData.setEndPointer(Resource.COLLISION,collisionBuffer.position());
                }
                if(dataBuffer.position() > metaData.getEndPointer(Resource.DATA)){
                    metaData.setEndPointer(Resource.DATA,dataBuffer.position());
                }
                collisionBuffer.putInt(record.position + key.size() + Integer.BYTES, colBubble);
            }
        }
    }

    private synchronized void createBubble(Resource resource, int position) {
        buffers[resource.ordinal()].position(metaData.getEndPointer(resource));
        buffers[resource.ordinal()].putInt(position);
        metaData.setEndPointer(resource,buffers[resource.ordinal()].position());
    }

    private synchronized int burstBubble(Resource resource) {
        Resource primaryResource = null;
        if (resource == Resource.COLLISION_BUBBLE) {
            primaryResource = Resource.COLLISION;
        } else if (resource == Resource.DATA_BUBBLE) {
            primaryResource = Resource.DATA;
        }
        assert primaryResource != null;
        int lastIndex = metaData.getEndPointer(resource) - Integer.BYTES;
        if (lastIndex < 0) {
           return metaData.getEndPointer(primaryResource);
        }
        metaData.setEndPointer(resource,lastIndex);
        return buffers[resource.ordinal()].getInt(lastIndex);
    }

    @Override
    public void close() {
        metaData.close();
        for(MappedByteBuffer buffer: buffers) {
            DBUtil.closeBuffer(buffer);
        }
    }
}
