package in.prasannathapa;

import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Key;
import in.prasannathapa.db.utils.RandomUtil;
import in.prasannathapa.db.utils.key.IP;
import in.prasannathapa.db.utils.key.ThreatData;

import javax.naming.SizeLimitExceededException;
import java.security.InvalidKeyException;
import java.util.stream.IntStream;

public class Main {

    static ThreatData generateThreat(Key key) {
        return new ThreatData(key.hash() % 100, RandomUtil.getRandomCategories(key.hash()), RandomUtil.allCategories);
    }

    public static void main(String[] args) throws Exception {
        int entries = 50_000_000;
        float loadFactor = 0.5f;
        try (HashDB db = HashDB.createDB(IP.LENGTH, ThreatData.LENGTH, entries, loadFactor, "WebrootATA")) {

            System.out.println("Dump Benchmark");
            for (int x = 1; x <= 1; x++) {
                //Benchmark this
                long startTime = System.nanoTime();
                IntStream.range(0, entries).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
                    try {
                        db.put(key, generateThreat(key));
                    } catch (SizeLimitExceededException | InvalidKeyException e) {
                        System.out.println(e.getMessage());
                    }
                });
                IntStream.range(0, entries/10).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
                    try {
                        db.remove(key);
                    } catch (InvalidKeyException e) {
                        System.out.println(e.getMessage());
                    }
                });
                long endTime = System.nanoTime() - startTime;
                RandomUtil.printStats(x, entries + entries/10, endTime);
            }
            //Benchmark this
            System.out.println("Read Benchmark");
            for (int x = 1; x <= 10; x++) {
                long startTime = System.nanoTime();
                IntStream.range(0, entries).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
                    try {
                        ThreatData data = ThreatData.readFrom(db.get(key));
                        //if (data != null && !data.toString(RandomUtil.allCategories).equals(generateThreat(key).toString(RandomUtil.allCategories))) {
                        //    System.out.println("Disparity Found!: " + data.toString(RandomUtil.allCategories) + " and " + generateThreat(key).toString(RandomUtil.allCategories));
                        //}
                    } catch (InvalidKeyException e) {
                        System.out.println(e.getMessage());
                    }
                });
                long endTime = System.nanoTime() - startTime;
                RandomUtil.printStats(x, entries, endTime);
            }
        }
    }

}