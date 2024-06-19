package in.prasannathapa.db.data;

public interface Key extends Value{
    boolean matches(byte[] anotherKey);
    int hash();
}
