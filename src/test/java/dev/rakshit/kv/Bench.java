package dev.rakshit.kv;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

/** Rough throughput numbers. Run with: ./build.sh --no-tests && java -cp out/main:out/test dev.rakshit.kv.Bench (after a normal ./build.sh). */
public final class Bench {
    public static void main(String[] args) throws Exception {
        byte[] value = new byte[100];
        new Random(1).nextBytes(value);

        Path d1 = Files.createTempDirectory("kv-bench");
        try (KvStore db = KvStore.open(d1, new Options(4 << 20, 4, false))) {
            int n = 200_000;
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) db.put(String.format("user:%08d", i), value);
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("puts, no fsync : %,9.0f ops/s  (%d puts, %d tables)%n", n / secs, n, db.tableCount());

            Random r = new Random(2);
            int reads = 100_000;
            t0 = System.nanoTime();
            for (int i = 0; i < reads; i++) db.get(String.format("user:%08d", r.nextInt(n)));
            secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("gets, present  : %,9.0f ops/s  (%d random hits)%n", reads / secs, reads);

            long rej0 = db.bloomRejections(), blk0 = db.blockReads();
            t0 = System.nanoTime();
            for (int i = 0; i < reads; i++) db.get(String.format("user:%08dx", i)); // absent, but inside the tables' key ranges
            secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("gets, absent   : %,9.0f ops/s  (%,d table checks: %,d rejected by bloom, %,d disk block reads)%n",
                    reads / secs, (long) reads * db.tableCount(), db.bloomRejections() - rej0, db.blockReads() - blk0);
        }

        Path d2 = Files.createTempDirectory("kv-bench");
        try (KvStore db = KvStore.open(d2, new Options(4 << 20, 4, true))) {
            int n = 2_000;
            long t0 = System.nanoTime();
            for (int i = 0; i < n; i++) db.put(String.format("user:%08d", i), value);
            double secs = (System.nanoTime() - t0) / 1e9;
            System.out.printf("puts, fsync    : %,9.0f ops/s  (%d puts, every write durable)%n", n / secs, n);
        }
    }
}
