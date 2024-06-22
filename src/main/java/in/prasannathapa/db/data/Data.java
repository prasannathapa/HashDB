package in.prasannathapa.db.data;

public class Data extends FixedRecord{
    public Data(int keySize) {
        super(keySize);
    }
    public Data(byte[] data) {
        super(data.length);
        update(data);
    }
    public Data(FixedRecord data) {
        super(data);
    }
    public Data() {}
    @Override
    protected void onUpdate(byte[] data) {}
}
