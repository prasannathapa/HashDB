import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.IP;

public class Main {

    public static void main(String[] args) throws Exception {

        HashDB<IP,IP> db = HashDB.createDB(IP.LENGTH, IP.LENGTH, 1000, "TestDB");

        IP ip1 = new IP("129.168.2.1");
        IP ip2 = new IP("129.168.2.2");

        db.put(ip1, new IP("99.99.29.19"));
        db.put(ip2,  new IP("0.0.0.0"));

        db.remove(ip2);

        Data data = db.get(ip1);
        System.out.println(new IP(data)); //99.99.29.19

        data = db.get(ip2); //null
        System.out.println(data); //Null

        db.close();
        db.delete();
    }

}