package in.prasannathapa.db.remote;

import in.prasannathapa.db.data.FixedRecord;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.Remote;

public interface RemoteHashDB<K extends  FixedRecord, V extends FixedRecord> extends Remote {
    public FixedRecord[] getAll(String dataSpace, K[] keys) throws IOException;
    public void putAll(String dataSpace, K[] keys, V[] values) throws IOException, SizeLimitExceededException;
    public FixedRecord[] removeAndGetAll(String dataSpace, K[] keys) throws IOException;
    public void removeAll(String dataSpace, K[] keys) throws IOException;
    public void createDB(String dataSpace, int keySize, int valueSize, int maxEntries) throws SizeLimitExceededException, IOException;
}
