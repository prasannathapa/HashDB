package in.prasannathapa.db.data;

import in.prasannathapa.db.data.Key;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;

public class CollisionRecord implements Key {

    public final byte[] key;
    public int pointer, next, position;
    public CollisionRecord(int size) {
        this.key = new byte[size];
    }
    @Override
    public int size() {
        return key.length + Integer.BYTES * 2; // Key size + 2 integers (4 bytes each)
    }

    @Override
    public byte[] write() {
        byte[] data = new byte[size()];
        System.arraycopy(key, 0, data, 0, key.length);
        System.arraycopy(intToBytes(pointer), 0, data, key.length, 4);
        System.arraycopy(intToBytes(next), 0, data, key.length + 4, 4);
        return data;
    }

    @Override
    public void read(ByteBuffer buffer) {
        position = buffer.position();
        buffer.get(key);
        pointer = buffer.getInt();
        next = buffer.getInt();
    }

    private static byte[] intToBytes(int value) {
        byte[] data = new byte[4];
        data[0] = (byte) (value >> 24);
        data[1] = (byte) (value >> 16);
        data[2] = (byte) (value >> 8);
        data[3] = (byte) (value & 0xFF);
        return data;
    }
    @Override
    public int hash() {
        return 0;
    }

    @Override
    public boolean matches(byte[] anotherKey) {
        return Arrays.equals(key, anotherKey);
    }

}
