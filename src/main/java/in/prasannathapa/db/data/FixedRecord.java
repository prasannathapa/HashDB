package in.prasannathapa.db.data;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

public abstract class FixedRecord implements Externalizable {
    protected byte[] data;
    private int size = 0;

    public FixedRecord(int size){
        this.data = new byte[size];
        this.size = data.length;
    }
    public FixedRecord(FixedRecord data){
        this.data = data.data;
        this.size = data.data.length;
        onUpdate(data.data);
    }
    public FixedRecord() {}

    public void update(byte[] update){
        assert update.length == size;
        System.arraycopy(update, 0, data, 0, size);
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
        return Arrays.hashCode(data);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(size);
        out.write(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        size = in.readInt();
        data = new byte[size];
        in.read(data);
        onUpdate(data);
    }
    public final int size(){
        return size;
    }
}
