import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.IP;

public class Main {

    public static void main(String[] args) throws Exception {
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
}