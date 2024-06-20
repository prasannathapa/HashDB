import in.prasannathapa.db.HashDB;
import in.prasannathapa.db.data.Data;
import utils.RandomUtil;
import utils.key.IP;
import utils.key.ThreatData;

import javax.naming.SizeLimitExceededException;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.util.stream.IntStream;

public class Main {

    static ThreatData generateThreat(Data key) {
        long hashCode = Integer.MAX_VALUE + (long) key.hashCode();
        return new ThreatData((int) (hashCode % 100), RandomUtil.getRandomCategories(hashCode), RandomUtil.allCategories);
    }

    void benchmarkLocal(String name, int entries) throws SizeLimitExceededException, IOException {
        try (HashDB<IP,ThreatData> db = HashDB.createDB(IP.LENGTH, ThreatData.LENGTH, entries, name)) {
            System.out.println("Dump Benchmark");
            for (int x = 1; x <= 1; x++) {
                //Benchmark this
                long startTime = System.nanoTime();
                IntStream.range(0, entries).sequential().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
                    try {
                        db.put(key, generateThreat(key));
                    } catch (SizeLimitExceededException e) {
                        System.out.println(e.getMessage());
                    }
                });
                //IntStream.range(0, entries/10).sequential().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(db::remove);
                long endTime = System.nanoTime() - startTime;
                RandomUtil.printStats(x, entries, endTime);
            }
            //Benchmark this
            System.out.println("Read Benchmark");
            for (int x = 1; x <= 10; x++) {
                long startTime = System.nanoTime();
                IntStream.range(0, entries).parallel().mapToObj(i -> new IP(RandomUtil.generateRandomIP())).forEach(key -> {
                    Data data = db.get(key);
                    if(data != null) {
                        ThreatData threatData = new ThreatData(data);
                        if (!threatData.toString(RandomUtil.allCategories).equals(generateThreat(key).toString(RandomUtil.allCategories))) {
                            System.out.println("Disparity Found!: " + threatData.toString(RandomUtil.allCategories) + " and " + generateThreat(key).toString(RandomUtil.allCategories));
                        }
                    }
                });
                long endTime = System.nanoTime() - startTime;
                RandomUtil.printStats(x, entries, endTime);
            }
        }
        HashDB.delete(name);
    }
    public static void main(String[] args) throws Exception {
        new Main().benchmarkLocal("ThreatTest",25_000_000);
    }

}