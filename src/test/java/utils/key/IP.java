package utils.key;

import in.prasannathapa.db.data.MappableData;


public class IP extends MappableData {

    public static final int LENGTH = Integer.BYTES;
    private int ip = 0;
    public IP(String ip) {
        super(LENGTH);
        String[] parts = ip.split("\\.");
        for (int i = 0; i < 4; i++) {
            int part = Integer.parseInt(parts[i]);
            data[i] = (byte) part;
            this.ip |= (part << (8 * (3 - i)));
        }
    }

    public IP() {
        super(LENGTH);
    }

    public String get(){
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
    public int mapToInt() {
        return ip;
    }
}
