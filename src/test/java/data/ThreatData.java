package data;

import in.prasannathapa.db.data.Data;
import in.prasannathapa.db.data.FixedRecord;

import java.io.*;
import java.util.*;

public class ThreatData extends Data {

    public static final int REP_LENGTH = 1;
    public static final int CAT_LENGTH = 1;
    public static final int LENGTH = REP_LENGTH + CAT_LENGTH;

    public ThreatData(int reputation, Set<String> categories, Collection<String> allCategories) {
        super(LENGTH);
        if (reputation < 0 || reputation > 100) {
            throw new IllegalArgumentException("Reputation must be between 0 and 100");
        } else if (categories.size() > allCategories.size()) {
            throw new IllegalArgumentException("Categories length should not exceed " + allCategories.size());
        }

        int index = 0;
        BitSet categoryFilter = new BitSet(CAT_LENGTH);
        for (String category : allCategories) {
            if (categories.contains(category)) {
                categoryFilter.set(index);
            }
            index++;
        }
        byte[] bitSetBytes = categoryFilter.toByteArray();
        data[0] = (byte) reputation;
        System.arraycopy(bitSetBytes, 0, data, 1, Math.min(bitSetBytes.length, CAT_LENGTH));
    }

    public ThreatData() {
        super(LENGTH);
    }

    public static ThreatData wrap(FixedRecord data) {
        if(data == null || data.size() != LENGTH){
            return null;
        }
        return new ThreatData(data);
    }

    public ThreatData(FixedRecord record) {
        super(record);
    }

    public int getReputation(){
        return data[0];
    }
    public List<String> getCategories(Iterable<String> allCategories) {
        List<String> setCategories = new ArrayList<>();
        BitSet categoryFilter = BitSet.valueOf(new byte[]{data[1]});
        int index = 0;
        for (String category : allCategories) {
            if (categoryFilter.get(index)) {
                setCategories.add(category);
            }
            index++;
        }
        return setCategories;
    }
    public String toString(Iterable<String> allCategories) {
        return "["+data[0]+"]"+Arrays.toString(getCategories(allCategories).toArray(new String[0]));
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.write(data);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException {
        in.read(data);
    }
}
