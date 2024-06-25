package in.prasannathapa.db.remote;

import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.FixedRecord;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

public class Server extends UnicastRemoteObject implements RemoteHashDB{

    public Server() throws RemoteException {
        super();
    }

    @Override
    public FixedRecord[] getAll(String dataSpace, FixedRecord[] keys) throws IOException {
        return HashDB.readFrom(dataSpace).getAll(keys);
    }

    @Override
    public void putAll(String dataSpace, FixedRecord[] keys, FixedRecord[] values) throws IOException, SizeLimitExceededException {
        HashDB.readFrom(dataSpace).putAll(keys, values);
    }

    @Override
    public FixedRecord[] removeAndGetAll(String dataSpace, FixedRecord[] keys) throws IOException {
        return HashDB.readFrom(dataSpace).removeAll(keys);
    }

    @Override
    public void removeAll(String dataSpace, FixedRecord[] keys) throws IOException {
        HashDB.readFrom(dataSpace).removeAll(keys);
    }

    @Override
    public void createDB(String dataSpace, int keySize, int valueSize, int maxEntries) throws SizeLimitExceededException, IOException {
        HashDB.createDB(keySize, valueSize, maxEntries, dataSpace);
    }

    @Override
    public void deleteDB(String dataSpace) throws IOException {
        HashDB.deleteDB(dataSpace);
    }

    public static void main(String[] args) {
        try {
            int port = 1099;
            if(args.length == 1){
                port = Integer.parseInt(args[0]);
            }
            Server obj = new Server();
            Registry registry = LocateRegistry.createRegistry(port);
            registry.bind(HashDB.class.getName(), obj);
            System.out.println("Server Started");
            System.out.println("bind: "+HashDB.class.getName() +" port: "+port);
        } catch (Exception e) {
            System.err.println("Server exception: " + e);
        }
    }
}
