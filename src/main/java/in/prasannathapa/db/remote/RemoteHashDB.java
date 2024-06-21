package in.prasannathapa.db.remote;

import in.prasannathapa.db.data.FixedRecord;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.Remote;

public interface RemoteHashDB extends Remote {
    public FixedRecord[] getAll(String dataSpace, FixedRecord[] keys) throws IOException;
    public void putAll(String dataSpace, FixedRecord[] keys, FixedRecord[] values) throws IOException, SizeLimitExceededException;
    public FixedRecord[] removeAndGetAll(String dataSpace, FixedRecord[] keys) throws IOException;
    public void removeAll(String dataSpace, FixedRecord[] keys) throws IOException;
    public void createDB(String dataSpace, int keySize, int valueSize, int maxEntries) throws SizeLimitExceededException, IOException;
    public void deleteDB(String dataSpace) throws IOException;
}
