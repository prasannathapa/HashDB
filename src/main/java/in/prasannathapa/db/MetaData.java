package in.prasannathapa.db;


import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

class MetaData implements AutoCloseable{

    private static final int NON_ARRAY_BYTES = Integer.BYTES * 3 + Float.BYTES;
    public static final int BYTES = NON_ARRAY_BYTES + Resource.values().length * Integer.BYTES;
    public final int keySize, valueSize, entries;
    private final float loadFactor;
    private final int[] endPointers = new int[Resource.values().length];

    public final int buckets;

    private MappedByteBuffer buffer;

    public MetaData(DBUtil dbUtil) throws IOException {
        this.buffer = dbUtil.getBuffer(Resource.META, FileChannel.MapMode.READ_WRITE);
        this.keySize = buffer.getInt();
        this.valueSize = buffer.getInt();
        this.entries = buffer.getInt();
        this.loadFactor = buffer.getFloat();
        for(Resource resource: Resource.values()){
            endPointers[resource.ordinal()] = buffer.getInt();
        }
        this.buckets = Math.min(Integer.MAX_VALUE/Integer.BYTES,(int)(entries/loadFactor));
    }

    public MetaData(int keySize, int valueSize, int entries, float loadFactor, DBUtil dbUtil) throws IOException {
        buffer = dbUtil.getBuffer(Resource.META, FileChannel.MapMode.READ_WRITE);
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.entries = entries;
        this.loadFactor = loadFactor;
        this.buckets = (int) Math.min(Integer.MAX_VALUE/Integer.BYTES,entries/loadFactor);

        buffer.putInt(keySize);
        buffer.putInt(valueSize);
        buffer.putInt(entries);
        buffer.putFloat(loadFactor);
        for(Resource resource: Resource.values()){
            endPointers[resource.ordinal()] = 0;
            buffer.putInt(0);
        }
    }

    public int getEndPointer(Resource resource){
        return endPointers[resource.ordinal()];
    }
    public synchronized void setEndPointer(Resource resource, int endPointer){
        endPointers[resource.ordinal()] =  endPointer;
        buffer.position(NON_ARRAY_BYTES + resource.ordinal() * Integer.BYTES);
        buffer.putInt(endPointer);
    }

    @Override
    public void close(){
        DBUtil.closeBuffer(buffer);
        buffer = null;
    }

    public float getLoadFactor() {
        return loadFactor;
    }
}
