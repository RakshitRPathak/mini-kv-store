package dev.rakshit.kv;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Bloom filter stored alongside each SSTable so a lookup can skip files that cannot contain the key.
 * "Definitely absent" costs a few memory reads instead of a disk read. No false negatives; false
 * positives only cost one wasted block read.
 *
 * <p>Sizing: for n items and false-positive rate p, m = -n ln p / (ln 2)^2 bits and k = (m/n) ln 2 probes.
 */
final class BloomFilter {

    private final long[] bits;
    private final long numBits;
    private final int numHashes;

    BloomFilter(int expectedInsertions, double falsePositiveRate) {
        int n = Math.max(1, expectedInsertions);
        double ln2 = Math.log(2);
        this.numBits = Math.max(64, (long) Math.ceil(-n * Math.log(falsePositiveRate) / (ln2 * ln2)));
        this.numHashes = Math.max(1, (int) Math.round((double) numBits / n * ln2));
        this.bits = new long[(int) ((numBits + 63) / 64)];
    }

    private BloomFilter(long numBits, int numHashes, long[] bits) {
        this.numBits = numBits;
        this.numHashes = numHashes;
        this.bits = bits;
    }

    void add(String key) {
        long[] h = hashes(key);
        for (int i = 0; i < numHashes; i++) {
            long pos = Math.floorMod(h[0] + (long) i * h[1], numBits);
            bits[(int) (pos >>> 6)] |= 1L << (pos & 63);
        }
    }

    boolean mightContain(String key) {
        long[] h = hashes(key);
        for (int i = 0; i < numHashes; i++) {
            long pos = Math.floorMod(h[0] + (long) i * h[1], numBits);
            if ((bits[(int) (pos >>> 6)] & (1L << (pos & 63))) == 0) return false;
        }
        return true;
    }

    long bitCount() {
        return numBits;
    }

    int hashCount() {
        return numHashes;
    }

    void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(numBits);
        out.writeInt(numHashes);
        out.writeInt(bits.length);
        for (long w : bits) out.writeLong(w);
    }

    static BloomFilter readFrom(DataInputStream in) throws IOException {
        long numBits = in.readLong();
        int numHashes = in.readInt();
        int words = in.readInt();
        if (numBits <= 0 || numHashes <= 0 || words != (numBits + 63) / 64) {
            throw new IOException("corrupt bloom filter header");
        }
        long[] bits = new long[words];
        for (int i = 0; i < words; i++) bits[i] = in.readLong();
        return new BloomFilter(numBits, numHashes, bits);
    }

    private static long[] hashes(String key) {
        try {
            byte[] d = MessageDigest.getInstance("MD5").digest(key.getBytes(StandardCharsets.UTF_8));
            long h1 = 0, h2 = 0;
            for (int i = 0; i < 8; i++) h1 = (h1 << 8) | (d[i] & 0xFF);
            for (int i = 8; i < 16; i++) h2 = (h2 << 8) | (d[i] & 0xFF);
            return new long[] {h1, h2 | 1};
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
