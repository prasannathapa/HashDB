import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Key;
import utils.RandomUtil;
import utils.key.IP;
import utils.key.ThreatData;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.stream.IntStream;

public class Main {

    static ThreatData generateThreat(Key key) {
        return new ThreatData(key.hash() % 100, RandomUtil.getRandomCategories(key.hash()), RandomUtil.allCategories);
    }

    void benchmarkLocal(int entries, float loadFactor) throws SizeLimitExceededException, IOException {
        try (HashDB db = HashDB.createDB(IP.LENGTH, ThreatData.LENGTH, entries, loadFactor, "ThreatTest")) {
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
            for (int x = 1; x <= 2; x++) {
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
            db.delete();
        }
    }
    public static void main(String[] args) throws Exception {
        new Main().benchmarkLocal(50_000_000,0.5f);
    }

}