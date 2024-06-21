package in.prasannathapa.db.data;

public abstract class MappableData extends Data {
    public MappableData(int size) {
        super(size);
    }
    public MappableData(Data data) {
        super(data);
    }
    abstract public int mapToInt();

    @Override
    public int hashCode(){
        return mapToInt();
    }
}
