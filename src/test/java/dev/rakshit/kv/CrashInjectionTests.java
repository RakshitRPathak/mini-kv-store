package dev.rakshit.kv;

import static dev.rakshit.kv.TestSupport.*;
import static dev.rakshit.kv.testkit.Assert.*;

import dev.rakshit.kv.testkit.Test;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

/**
 * Kills the store at each step of flush and compaction (via {@link Hooks}), reopens the directory, and
 * checks the data equals a reference model. This is what turns "I ordered the writes carefully" into evidence.
 *
 * <p>The model is updated <em>before</em> each operation because every injected crash happens after the
 * operation's log record is written: from that moment the write is durable and must survive.
 */
public class CrashInjectionTests {

    private static final List<String> POINTS = List.of(
            "flush.afterTableWritten", "flush.afterTableRenamed", "flush.afterManifest", "flush.done",
            "compact.afterTableWritten", "compact.afterTableRenamed", "compact.afterManifest", "compact.done");

    private static List<String> universe() {
        List<String> u = new ArrayList<>();
        for (int i = 0; i < 40; i++) u.add(String.format("key%02d", i));
        return u;
    }

    /** Runs a random workload until the armed crash fires (or the workload ends), then recovers and checks. */
    private static boolean runOnce(String point, int skip, long seed) throws Exception {
        Path dir = tempDir();
        Random rnd = new Random(seed);
        TreeMap<String, byte[]> model = new TreeMap<>();
        List<String> universe = universe();
        CrashAt crash = new CrashAt(point, skip);
        KvStore db = KvStore.open(dir, tiny(), crash);

        for (int step = 0; step < 600 && !crash.fired; step++) {
            String key = universe.get(rnd.nextInt(universe.size()));
            boolean delete = rnd.nextInt(100) < 25;
            byte[] value = b("v" + step);
            if (delete) model.remove(key); else model.put(key, value);
            try {
                if (delete) db.delete(key); else db.put(key, value);
            } catch (Hooks.SimulatedCrash expected) {
                // fall out: the loop condition sees crash.fired
            }
        }
        db.abandon(); // the process "died": no orderly close

        try (KvStore recovered = KvStore.open(dir, tiny())) {
            assertMatches(recovered, model, universe);
            // The recovered store must also keep working, including through further flushes and compactions.
            for (int step = 0; step < 200; step++) {
                String key = universe.get(rnd.nextInt(universe.size()));
                if (rnd.nextInt(100) < 25) { recovered.delete(key); model.remove(key); }
                else { byte[] v = b("after" + step); recovered.put(key, v); model.put(key, v); }
            }
            assertMatches(recovered, model, universe);
        }
        try (KvStore again = KvStore.open(dir, tiny())) {
            assertMatches(again, model, universe);
        }
        return crash.fired;
    }

    @Test
    void crashAtEveryStepOfFlushAndCompactionLosesNothing() throws Exception {
        for (String point : POINTS) {
            int fired = 0, runs = 0;
            for (long seed = 1; seed <= 12; seed++) {
                for (int skip = 0; skip < 3; skip++) {
                    runs++;
                    try {
                        if (runOnce(point, skip, seed * 31 + skip)) fired++;
                    } catch (AssertionError e) {
                        throw new AssertionError("crash at " + point + " (skip " + skip + ", seed " + seed + "): " + e.getMessage());
                    }
                }
            }
            assertTrue(fired > runs / 2, "crash point " + point + " was reached in only " + fired + " of " + runs + " runs");
        }
    }

    /** Sets up three tables where "k" is live in the oldest, deleted in the middle one, then crashes the compaction. */
    private static Path crashedCompaction(String point, Options o) throws Exception {
        Path dir = tempDir();
        CrashAt crash = new CrashAt(point, 0);
        KvStore db = KvStore.open(dir, o, crash);
        db.put("k", "alive");
        db.put("keep", "yes");
        db.flush();                          // table 1 holds the value
        db.delete("k");
        db.flush();                          // table 2 holds the tombstone
        db.put("other", "x");
        try {
            db.flush();                      // table 3: reaches the threshold, so compaction runs and crashes
        } catch (Hooks.SimulatedCrash expected) {
            // expected
        }
        assertTrue(crash.fired, "crash point " + point + " was never reached");
        db.abandon();
        return dir;
    }

    @Test
    void deletedKeysStayDeletedWhenCompactionCrashes() throws Exception {
        Options o = new Options(1 << 20, 3, false);
        for (String point : List.of("compact.afterTableWritten", "compact.afterTableRenamed", "compact.afterManifest", "compact.done")) {
            Path dir = crashedCompaction(point, o);
            try (KvStore recovered = KvStore.open(dir, o)) {
                assertFalse(recovered.get("k").isPresent(), "deleted key 'k' came back after a crash at " + point);
                assertEquals("yes", recovered.getString("keep").get());
                assertEquals("x", recovered.getString("other").get());
            }
        }
    }

    /**
     * Why the MANIFEST exists. Once the merged table is committed, the old tables are garbage, but a crash can leave
     * <em>any subset</em> of them on disk, because a filesystem does not promise that file deletions become permanent
     * in the order they were issued. Here the tombstone's table is gone but the table holding the old value survived.
     * The merged table has already dropped the tombstone, so a store that simply loaded every *.sst file it found
     * would resurrect "k". The manifest says which tables are live, so the survivor is recognised as garbage.
     */
    @Test
    void deletedKeysStayDeletedWhenOnlySomeOldTablesWereUnlinked() throws Exception {
        Options o = new Options(1 << 20, 3, false);
        Path dir = crashedCompaction("compact.afterManifest", o);
        java.nio.file.Files.delete(dir.resolve("table-00000002.sst")); // the tombstone's table vanished, the value's did not
        assertTrue(java.nio.file.Files.exists(dir.resolve("table-00000001.sst")), "test setup: the old value table must remain");
        try (KvStore recovered = KvStore.open(dir, o)) {
            assertFalse(recovered.get("k").isPresent(), "deleted key 'k' was resurrected from a stale table");
            assertEquals("yes", recovered.getString("keep").get());
            assertEquals("x", recovered.getString("other").get());
            assertEquals(1, recovered.tableCount());
        }
    }

    @Test
    void crashDuringFlushKeepsTheWriteThatTriggeredIt() throws Exception {
        for (String point : List.of("flush.afterTableWritten", "flush.afterTableRenamed", "flush.afterManifest")) {
            Path dir = tempDir();
            CrashAt crash = new CrashAt(point, 0);
            KvStore db = KvStore.open(dir, new Options(100, 10, false), crash);
            db.put("first", "1");
            try {
                db.put("second", "x".repeat(200)); // pushes the memtable past 100 bytes -> flush -> crash
            } catch (Hooks.SimulatedCrash expected) {
                // expected
            }
            db.abandon();
            try (KvStore recovered = KvStore.open(dir, new Options(100, 10, false))) {
                assertEquals("1", recovered.getString("first").get());
                assertEquals(200, recovered.get("second").get().length);
            }
        }
    }

    @Test
    void afterRecoveryOnlyLiveFilesRemain() throws Exception {
        Path dir = tempDir();
        CrashAt crash = new CrashAt("compact.afterManifest", 0);
        KvStore db = KvStore.open(dir, new Options(1 << 20, 3, false), crash);
        for (int t = 0; t < 3; t++) {
            db.put("k" + t, "v");
            try {
                db.flush();
            } catch (Hooks.SimulatedCrash expected) {
                // third flush triggers the compaction that crashes
            }
        }
        db.abandon();
        assertTrue(filesIn(dir).stream().filter(f -> f.endsWith(".sst")).count() > 1, "expected old tables left behind by the crash");
        try (KvStore recovered = KvStore.open(dir, new Options(1 << 20, 3, false))) {
            assertEquals(1, recovered.tableCount());
        }
        assertEquals(1L, filesIn(dir).stream().filter(f -> f.endsWith(".sst")).count());
    }
}
