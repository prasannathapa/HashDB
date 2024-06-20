package in.prasannathapa.db;


import in.prasannathapa.db.data.Data;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

class MetaData extends Data implements AutoCloseable{

    private static final int DATA_BYTES = Integer.BYTES * 3;
    public static final int ARRAY_BYTES =  Resource.values().length * Integer.BYTES;
    public static final int BYTES = DATA_BYTES + ARRAY_BYTES;
    private static final float LOAD_FACTOR = 0.6F;
    private int keySize, valueSize, entries, buckets;
    private long dataFileSizeLimit;
    private final int[] endPointers = new int[Resource.values().length];
    private final MappedByteBuffer fileBuffer;
    public MetaData(MappedByteBuffer fileBuffer)  {
        super(BYTES);
        this.fileBuffer = fileBuffer;
        read();
    }

    public MetaData(int keySize, int valueSize, int entries, MappedByteBuffer fileBuffer) {
        super(BYTES);
        this.fileBuffer = fileBuffer;
        this.keySize = keySize;
        this.valueSize = valueSize;
        this.entries = entries;
        this.buckets = getBucketSize(entries);
        this.dataFileSizeLimit = (long) entries * (keySize + valueSize);
        assert dataFileSizeLimit <= Integer.MAX_VALUE;
        ByteBuffer heapBuffer = ByteBuffer.wrap(data);
        heapBuffer.putInt(keySize);
        heapBuffer.putInt(valueSize);
        heapBuffer.putInt(entries);
        for(Resource resource: Resource.values()){
            heapBuffer.putInt(endPointers[resource.ordinal()]);
        }
        write();
    }

    public static int getBucketSize(int entries){
        entries = Math.min(MAX_PRIME,(int)(entries/LOAD_FACTOR));
        return nextPrime(entries);
    }
    //value & Integer.MAX_VALUE makes it positive
    public int getEndPointer(Resource resource){
        return Math.abs(endPointers[resource.ordinal()]);
    }
    public synchronized void setEndPointer(Resource resource, int endPointer){
        endPointers[resource.ordinal()] =  -endPointer;
    }

    public int getBucket(Data key) {
        return (int) (((long)Integer.MAX_VALUE + key.hashCode()) % buckets);
    }

    public void update(){
        int pos = DATA_BYTES;
        for(Resource resource: Resource.values()){
            if(endPointers[resource.ordinal()] < 0){
                endPointers[resource.ordinal()] = -endPointers[resource.ordinal()];
                fileBuffer.putInt(pos, endPointers[resource.ordinal()]);
            }
            pos += Integer.BYTES;
        }
    }

    public int getDataFileSizeLimit() {
        return (int) dataFileSizeLimit;
    }

    public int getKeySize() {
        return keySize;
    }

    public static boolean isPrime(int number) {
        if (number <= 1) return false;
        if (number == 2 || number == 3) return true;
        if (number % 2 == 0 || number % 3 == 0) return false;

        for (int i = 5; i * i <= number; i += 6) {
            if (number % i == 0 || number % (i + 2) == 0) return false;
        }
        return true;
    }

    private static final int MAX_PRIME = 536870909;
    public static int nextPrime(int n) {
        if (n >= MAX_PRIME || n < 0) return MAX_PRIME;
        if (n <= 2) return 2;
        if (n == 3) return 3;

        int candidate = n % 2 == 0 ? n + 1 : n;

        while (!isPrime(candidate)) {
            candidate += 2;
        }
        return candidate;
    }


    public void read() {
        super.read(fileBuffer,0);
    }

    public void write() {
        super.write(fileBuffer,0);
    }

    @Override
    protected void onUpdate(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.keySize = buffer.getInt();
        this.valueSize = buffer.getInt();
        this.entries = buffer.getInt();
        for(Resource resource: Resource.values()){
            endPointers[resource.ordinal()] = buffer.getInt();
        }
        this.buckets = getBucketSize(entries);
        this.dataFileSizeLimit = (long) entries * (keySize + valueSize);
        assert dataFileSizeLimit <= Integer.MAX_VALUE;
    }

    public int getBuckets() {
        return buckets;
    }

    @Override
    public void close() {
        write();
        DBUtil.closeBuffer(fileBuffer);
    }

    public int getEntries() {
        return entries;
    }

    public int getValueSize() {
        return valueSize;
    }
}
