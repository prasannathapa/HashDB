package in.prasannathapa.db.data;

import java.nio.ByteBuffer;

public class BucketNode extends Data {
    public static final int NOT_WRITTEN = -1;
    public static final int BYTES = Integer.BYTES * 2;
    public int dataPointer, previousPosition, currentPosition, nextPosition;
    private final Data key;
    private boolean keyCached = false;
    private final ByteBuffer dataBuffer, collisionBuffer;
    public BucketNode(int keySize, int head, ByteBuffer dataBuffer, ByteBuffer collisionBuffer) {
        super(BYTES);
        this.key = new Data(keySize);
        this.dataBuffer = dataBuffer;
        this.collisionBuffer = collisionBuffer;
        this.currentPosition = NOT_WRITTEN;
        read(collisionBuffer, head);
    }

    public void readNext(){
        super.read(collisionBuffer, nextPosition);
        keyCached = false;
    }
    public boolean hasNext(){
        return nextPosition != NOT_WRITTEN;
    }


    @Override
    public boolean matches(Data otherKey) {
        if(!keyCached) {
            key.read(dataBuffer,dataPointer);
            keyCached = true;
        }
        return key.matches(otherKey);
    }

    @Override
    protected void onUpdate(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        previousPosition = currentPosition;
        currentPosition = nextPosition;
        nextPosition = buffer.getInt();
        dataPointer = buffer.getInt();
    }

    public int deleteNode() {
        collisionBuffer.putInt(previousPosition,nextPosition);
        return currentPosition;
    }

    public void updateNext(int nextPosition) {
        this.nextPosition = nextPosition;
        collisionBuffer.putInt(currentPosition,nextPosition);
    }
    public void updatePrevious(int nextPosition) {
        collisionBuffer.putInt(previousPosition,nextPosition);
    }
    public void updateData(Data key, Data value){
        dataBuffer.position(dataPointer);
        key.write(dataBuffer);
        value.write(dataBuffer);
    }
}
