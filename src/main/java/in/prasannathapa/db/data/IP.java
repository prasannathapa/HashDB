package in.prasannathapa.db.data;

import java.nio.ByteBuffer;


public class IP extends Data {

    public static final int LENGTH = Integer.BYTES;
    public IP(String ip) {
        super(LENGTH);
        String[] parts = ip.split("\\.");
        for (int i = 0; i < LENGTH; i++) {
            int part = Integer.parseInt(parts[i]);
            data[i] = (byte) part;
        }
    }
    public static IP wrap(FixedRecord data) {
        if(data == null) {
            return null;
        }
        return new IP(data.data);
    }

    private IP(byte[] data) {
        super(data);
    }

    public String toString(){
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 4; i++) {
            int octet = data[i] & 0xFF;
            sb.append(octet);
            if (i < 3) {
                sb.append(".");
            }
        }
        return sb.toString();
    }

    @Override
    public int hashCode() {
        return ByteBuffer.wrap(data).getInt();
    }
}
