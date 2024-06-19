package in.prasannathapa.db.utils.key;

import in.prasannathapa.db.data.Key;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class IP implements Key {
    public final byte[] bytes = new byte[Integer.BYTES];

    public IP(String ip) {
        String[] parts = ip.split("\\.");
        for (int i = 0; i < 4; i++) {
            int value = Integer.parseInt(parts[i]);
            bytes[i] = (byte) value;
        }
    }

    public static final int LENGTH = Integer.BYTES;

    public IP() {}

    @Override
    public boolean matches(byte[] anotherKey) {
        return Arrays.equals(bytes,anotherKey);
    }

    @Override
    public int size() {
        return Integer.BYTES;
    }

    public String get(){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            int octet = bytes[i] & 0xFF;
            sb.append(octet);
            if (i < 3) {
                sb.append(".");
            }
        }
        return sb.toString();
    }
    @Override
    public byte[] write() {
        return bytes.clone();
    }

    @Override
    public void read(ByteBuffer buffer) {
        buffer.get(bytes);
    }

    @Override
    public int hash() {
        int hash = 0;
        for (byte b : bytes) {
            hash = (hash << 8) | (hash >>> 24); // Rotate left by 8 bits
            hash += Byte.toUnsignedInt(b);
        }
        return Math.abs(hash);
    }
}
