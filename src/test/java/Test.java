import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.FixedRecord;
import in.prasannathapa.db.data.IP;
import in.prasannathapa.db.remote.RemoteHashDB;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;

public class Test {

    public static String host = "prasanna-14854-t";
    public static int port = 1090;
    public static void remoteTest() throws IOException, NotBoundException, SizeLimitExceededException {
        RemoteHashDB client = (RemoteHashDB) LocateRegistry.getRegistry(host, port).lookup(HashDB.class.getName());
        String dbName = "TestDB";
        client.createDB(dbName, IP.LENGTH, IP.LENGTH, 1000);

        IP[] keys = new IP[]{new IP("129.168.2.1"), new IP("129.168.2.2")};
        IP[] values = new IP[]{new IP("0.0.0.0"), new IP("1.1.1.1")};

        client.putAll(dbName,keys,values);

        FixedRecord[] removedValues = client.removeAndGetAll(dbName, new FixedRecord[]{keys[1]});
        System.out.println(IP.wrap(removedValues[0])); //0.0.0.0

        IP data = IP.wrap(client.getAll(dbName, new FixedRecord[]{keys[0]})[0]);
        System.out.println(data); //1.1.1.1

        data = IP.wrap(client.getAll(dbName, new FixedRecord[]{keys[1]})[0]);
        System.out.println(data); //Null
    }
    public static void localTest() throws SizeLimitExceededException, IOException {
        String dbName = "TestDB";
        HashDB<IP, IP> db = HashDB.createDB(IP.LENGTH, IP.LENGTH, 1000, dbName);

        IP ip1 = new IP("129.168.2.1");
        IP ip2 = new IP("129.168.2.2");

        db.put(ip1, new IP("99.99.29.19"));
        db.put(ip2, new IP("0.0.0.0"));
        HashDB.closeDB(dbName);

        db = HashDB.readFrom(dbName);
        System.out.println(IP.wrap(db.remove(ip2))); //0.0.0.0

        IP data = IP.wrap(db.get(ip1));
        System.out.println(data); //99.99.29.19

        data = IP.wrap(db.get(ip2));
        System.out.println(data); //Null

        HashDB.deleteDB(dbName);
    }
    public static void main(String[] args) throws Exception {
        remoteTest();
        localTest();
    }
}