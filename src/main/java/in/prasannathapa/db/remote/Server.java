package in.prasannathapa.db.remote;

import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.FixedRecord;
import in.prasannathapa.db.data.IP;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Server<K extends FixedRecord,V extends  FixedRecord> extends UnicastRemoteObject implements RemoteHashDB<K,V>{

    protected Server() throws RemoteException {
        super();
    }

    @Override
    public FixedRecord[] getAll(String dataSpace, K[] keys) throws IOException {
        return HashDB.readFrom(dataSpace).getAll(keys);
    }

    @Override
    public void putAll(String dataSpace, K[] keys, V[] values) throws IOException, SizeLimitExceededException {
        HashDB.readFrom(dataSpace).putAll(keys, values);
    }

    @Override
    public FixedRecord[] removeAndGetAll(String dataSpace, K[] keys) throws IOException {
        return HashDB.readFrom(dataSpace).removeAll(keys);
    }

    @Override
    public void removeAll(String dataSpace, K[] keys) throws IOException {
        HashDB.readFrom(dataSpace).removeAll(keys);
    }

    @Override
    public void createDB(String dataSpace, int keySize, int valueSize, int maxEntries) throws SizeLimitExceededException, IOException {
        HashDB.createDB(keySize, valueSize, 1000, dataSpace);
    }

    public static void main(String[] args) {
        try {
            // Create an instance of HelloImpl
            Server<FixedRecord,FixedRecord> obj = new Server<>();
            // Bind the remote object's stub in the registry
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.bind(HashDB.class.getSimpleName(), obj);
            System.out.println("Server Started");
        } catch (Exception e) {
            System.err.println("Server exception: " + e);
        }
    }
}
