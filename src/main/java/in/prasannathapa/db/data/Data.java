package in.prasannathapa.db.data;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class Data {
    protected final byte[] data;
    public final int BYTES;

    public Data(int size){
        this.data = new byte[size];
        this.BYTES = data.length;
    }

    public Data(Data data) {
        //clone??
        this.data = data.data;
        this.BYTES = data.data.length;
    }

    public void update(byte[] update){
        assert update.length == BYTES;
        System.arraycopy(update, 0, data, 0, BYTES);
    }

    protected void onUpdate(byte[] data){};
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
    public boolean matches(Data data) {
        return Arrays.equals(this.data, data.data);
    }
    public int hashCode(){
        return Arrays.hashCode(data);
    }
}
