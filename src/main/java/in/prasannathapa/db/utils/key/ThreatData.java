package in.prasannathapa.db.utils.key;

import in.prasannathapa.db.data.Value;

import java.nio.ByteBuffer;
import java.util.*;

public class ThreatData implements Value {

    public static final int LENGTH = 3;
    private final byte[] data = new byte[LENGTH];
    public ThreatData(int reputation, Set<String> categories, Collection<String> allCategories) {
        if (reputation < 0 || reputation > 100) {
            throw new IllegalArgumentException("Reputation must be between 0 and 100");
        } else if (categories.size() > allCategories.size()) {
            throw new IllegalArgumentException("Categories length should not exceed " + allCategories.size());
        }

        int index = 0;
        BitSet categoryFilter = new BitSet(2 * Byte.SIZE);
        for (String category : allCategories) {
            if (categories.contains(category)) {
                categoryFilter.set(index);
            }
            index++;
        }
        byte[] bitSetBytes = categoryFilter.toByteArray();
        data[0] = (byte) reputation;
        System.arraycopy(bitSetBytes, 0, data, 1, Math.min(bitSetBytes.length, 2));
    }
    public ThreatData() {}

    public static ThreatData readFrom(ByteBuffer byteBuffer) {
        ThreatData threatData = null;
        if(byteBuffer != null){
            threatData = new ThreatData();
            threatData.read(byteBuffer);
        }
        return threatData;
    }

    public int getReputation(){
        return data[0];
    }
    public List<String> getCategories(Iterable<String> allCategories) {
        List<String> setCategories = new ArrayList<>();
        BitSet categoryFilter = BitSet.valueOf(new byte[]{data[1], data[2]});
        int index = 0;
        for (String category : allCategories) {
            if (categoryFilter.get(index)) {
                setCategories.add(category);
            }
            index++;
        }
        return setCategories;
    }
    @Override
    public int size() {
        return data.length;
    }

    @Override
    public byte[] write() {
        return data.clone(); // Return a copy to avoid unintended modifications
    }

    @Override
    public void read(ByteBuffer buffer) {
        buffer.get(data); // Read bytes directly into the data array
    }

    public String toString(Iterable<String> allCategories) {
        return "["+data[0]+"]"+Arrays.toString(getCategories(allCategories).toArray(new String[0]));
    }
}
