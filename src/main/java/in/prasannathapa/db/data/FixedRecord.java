package in.prasannathapa.db.data;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class FixedRecord implements Externalizable {
    private static final long serialVersionUID = 1L;

    protected byte[] data;
    private static final HashFunction MURMUR3_128 = Hashing.murmur3_128();

    public FixedRecord(int size){
        this.data = new byte[size];
    }
    public FixedRecord(FixedRecord data){
        this.data = data.data;
        onUpdate(data.data);
    }
    public FixedRecord() {}

    public void update(byte[] update){
        assert update.length == data.length;
        System.arraycopy(update, 0, data, 0, data.length);
    }

    protected abstract void onUpdate(byte[] data);
    public final void write(ByteBuffer buffer, int position) {
        buffer.position(position).put(data);
    }
    public final void write(ByteBuffer buffer) {
        buffer.put(data);
    }
    public final void read(ByteBuffer buffer, int position) {
        buffer.position(position).get(data);
        onUpdate(data);
    }
    public final void read(ByteBuffer buffer) {
        buffer.get(data);
        onUpdate(data);
    }
    public boolean matches(FixedRecord data) {
        return Arrays.equals(this.data, data.data);
    }
    public int hashCode(){
        return  MURMUR3_128.hashBytes(data).asInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(data.length);
        out.write(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        int size = in.readInt();
        data = new byte[size];
        in.readFully(data);
        onUpdate(data);
    }
    public final int size(){
        return data.length;
    }
}
