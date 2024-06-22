package in.prasannathapa.db.data;

public class Data extends FixedRecord{
    private static final long serialVersionUID = 2;
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
