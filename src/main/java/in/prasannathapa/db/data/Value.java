package in.prasannathapa.db.data;

import java.nio.ByteBuffer;

public interface Value {
    int size();
    byte [] write();
    void read(ByteBuffer buffer);
}
