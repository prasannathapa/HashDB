package utils;

import java.util.Random;

public class SequenceGenerator {
    private final Random random;
    private final int size;

    /**
     * Constructs a SequenceGenerator with a seed and size.
     *
     * @param seed The seed for random number generation.
     * @param size The size of the byte array to generate.
     */
    public SequenceGenerator(long seed, int size) {
        this.random = new Random(seed);
        this.size = size;
    }

    /**
     * Generates the next key byte array based on the random sequence.
     *
     * @return A byte array of specified size.
     */
    public byte[] getNextKey() {
        byte[] key = new byte[size];
        random.nextBytes(key);
        return key;
    }
    /**
     * Utility method to convert byte array to hexadecimal string.
     *
     * @param bytes The byte array to convert.
     * @return Hexadecimal representation of the byte array.
     */
    public static String bytesToHex(byte[] bytes) {
        StringBuilder hexString = new StringBuilder();
        for (byte b : bytes) {
            String hex = Integer.toHexString(0xff & b);
            if (hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }
}
