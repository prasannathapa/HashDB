package in.prasannathapa.db;

import in.prasannathapa.db.data.BucketNode;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

import static in.prasannathapa.db.data.BucketNode.NOT_WRITTEN;

class DBWriter<K extends FixedRecord, V extends FixedRecord> implements AutoCloseable{

    public final MetaData metaData;
    private final MappedByteBuffer[] buffers = new MappedByteBuffer[Resource.values().length];

    public DBWriter(MetaData metaData, DBUtil dbUtil) throws IOException {
        this.metaData = metaData;
        for(Resource resource: Resource.values()){
            if(resource != Resource.META){
                this.buffers[resource.ordinal()] = dbUtil.getBuffer(resource, FileChannel.MapMode.READ_WRITE);
            }
        }
    }

    public DBWriter(MetaData metaData, DBUtil dbUtil, boolean clearIndex) throws IOException {
        this(metaData, dbUtil);
        for (int i = 0; i < metaData.getBuckets() && clearIndex; i++) {
            buffers[Resource.INDEX.ordinal()].putInt(NOT_WRITTEN);
        }
    }

    public synchronized void remove(K key) {
        Data dbKey = new Data(metaData.getKeySize());
        int bucket = metaData.getBucket(key);
        int bucketPointer = bucket * Integer.BYTES;
        MappedByteBuffer idxBuffer = buffers[Resource.INDEX.ordinal()].position(bucketPointer);
        MappedByteBuffer dataBuffer = buffers[Resource.DATA.ordinal()];
        int pos = idxBuffer.getInt();
        if(pos == NOT_WRITTEN){
            return;
        } else if(pos >= 0) {
            dbKey.read(dataBuffer,pos);
            if(dbKey.matches(key)) {
                idxBuffer.putInt(bucketPointer, NOT_WRITTEN);
                createBubble(Resource.DATA_BUBBLE, pos);
            }
        } else {
            pos = (pos == Integer.MIN_VALUE ? 0 : -pos);
            MappedByteBuffer collisionBuffer = buffers[Resource.COLLISION.ordinal()];
            BucketNode linkedList = new BucketNode(metaData.getKeySize(), pos,dataBuffer,collisionBuffer);
            // Handle removal of the head node
            if(linkedList.matches(key)) {
                //if -1, write -1 else invert to neg pointer
                idxBuffer.putInt(bucketPointer,-Math.abs(linkedList.nextPosition));
                createBubble(Resource.COLLISION_BUBBLE,linkedList.currentPosition);
                return;
            }
            // Traverse the linked list to find and remove the node
            while (linkedList.hasNext()) {
                linkedList.readNext();
                if (linkedList.matches(key)) {
                    createBubble(Resource.COLLISION_BUBBLE, linkedList.deleteNode());
                    return;
                }
            }
        }
        metaData.update();
    }
    public synchronized void put(K key, V value) throws SizeLimitExceededException {
        int bubble;
        int bucket = metaData.getBucket(key);
        int bucketPointer = bucket * Integer.BYTES;
        MappedByteBuffer idxBuffer = buffers[Resource.INDEX.ordinal()];
        MappedByteBuffer dataBuffer = buffers[Resource.DATA.ordinal()];
        MappedByteBuffer collisionBuffer = buffers[Resource.COLLISION.ordinal()];

        int pos = idxBuffer.getInt(bucketPointer);
        if(pos == NOT_WRITTEN){
            bubble = burstBubble(Resource.DATA_BUBBLE);
            if(bubble >= metaData.getDataFileSizeLimit()){
                throw new SizeLimitExceededException("Max entries reached");
            }
            dataBuffer.position(bubble);
            key.write(dataBuffer);
            value.write(dataBuffer);
            idxBuffer.putInt(bucketPointer,bubble);
            int position =dataBuffer.position();
            if(metaData.getEndPointer(Resource.DATA) < position){
                metaData.setEndPointer(Resource.DATA, position);
            }
        } else if (pos >= 0){
            Data existingKey = new Data(metaData.getKeySize());
            existingKey.read(dataBuffer,pos);
            if(key.matches(existingKey)){
                value.write(dataBuffer);
                //Its overwrite no need to update end pointer
            } else {
                bubble = burstBubble(Resource.DATA_BUBBLE);
                if(bubble >= metaData.getDataFileSizeLimit()){
                    throw new SizeLimitExceededException("Max entries reached");
                }
                int colBubble = burstBubble(Resource.COLLISION_BUBBLE);
                idxBuffer.putInt(bucketPointer, colBubble == 0 ? Integer.MIN_VALUE : -colBubble);
                collisionBuffer.position(colBubble)
                        .putInt(NOT_WRITTEN).putInt(pos);
                if (collisionBuffer.position() > metaData.getEndPointer(Resource.COLLISION)) {
                    metaData.setEndPointer(Resource.COLLISION, collisionBuffer.position());
                }
                int nextBubble = burstBubble(Resource.COLLISION_BUBBLE);
                collisionBuffer.position(colBubble)
                        .putInt(nextBubble) //Replace End Node
                        .position(nextBubble)
                        .putInt(NOT_WRITTEN).putInt(bubble);
                if (collisionBuffer.position() > metaData.getEndPointer(Resource.COLLISION)) {
                    metaData.setEndPointer(Resource.COLLISION, collisionBuffer.position());
                }
                dataBuffer.position(bubble);
                key.write(dataBuffer);
                value.write(dataBuffer);
                if(dataBuffer.position() > metaData.getEndPointer(Resource.DATA)){
                    metaData.setEndPointer(Resource.DATA,dataBuffer.position());
                }
            }
        } else {
            pos = (pos == Integer.MIN_VALUE ? 0 : -pos);
            BucketNode record = new BucketNode(metaData.getKeySize(), pos,dataBuffer,collisionBuffer);
            while(record.hasNext() && !record.matches(key)) {
                record.readNext();
            }
            //Key exists, update existing record
            if(record.matches(key)){
                record.updateData(key,value);
                //No need to update end pointer since its overwrite
            } else {
                //Insert new Record
                bubble = burstBubble(Resource.DATA_BUBBLE);
                if(bubble >= metaData.getDataFileSizeLimit()){
                    throw new SizeLimitExceededException("Max entries reached");
                }
                int colBubble = burstBubble(Resource.COLLISION_BUBBLE);
                dataBuffer.position(bubble);
                key.write(dataBuffer);
                value.write(dataBuffer);
                collisionBuffer.position(colBubble)
                        .putInt(NOT_WRITTEN).putInt(bubble);
                if(collisionBuffer.position() > metaData.getEndPointer(Resource.COLLISION)){
                    metaData.setEndPointer(Resource.COLLISION,collisionBuffer.position());
                }
                if(dataBuffer.position() > metaData.getEndPointer(Resource.DATA)){
                    metaData.setEndPointer(Resource.DATA,dataBuffer.position());
                }
                record.updateNext(colBubble);
            }
        }
        metaData.update();
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
        for (MappedByteBuffer buffer : buffers) {
            DBUtil.closeBuffer(buffer);
        }
    }
}
