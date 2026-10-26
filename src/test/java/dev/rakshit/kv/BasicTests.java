package dev.rakshit.kv;

import static dev.rakshit.kv.TestSupport.*;
import static dev.rakshit.kv.testkit.Assert.*;

import dev.rakshit.kv.testkit.Test;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class BasicTests {

    @Test
    void putGetOverwriteDelete() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), tiny())) {
            db.put("a", "1");
            assertEquals("1", db.getString("a").orElse(null));
            db.put("a", "2");
            assertEquals("2", db.getString("a").orElse(null));
            db.delete("a");
            assertFalse(db.get("a").isPresent());
            assertFalse(db.get("never-existed").isPresent());
            db.delete("never-existed"); // deleting an absent key is allowed
        }
    }

    @Test
    void emptyValueIsDistinctFromDelete() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, tiny())) {
            db.put("empty", new byte[0]);
            db.put("gone", "x");
            db.delete("gone");
            db.flush();
        }
        try (KvStore db = KvStore.open(dir, tiny())) {
            assertTrue(db.get("empty").isPresent(), "an empty value is still a value");
            assertEquals(0, db.get("empty").get().length);
            assertFalse(db.get("gone").isPresent());
        }
    }

    @Test
    void unicodeKeysAndBinaryValues() throws Exception {
        Path dir = tempDir();
        byte[] binary = new byte[256];
        for (int i = 0; i < 256; i++) binary[i] = (byte) i;
        try (KvStore db = KvStore.open(dir, tiny())) {
            db.put("नमस्ते", binary);
            db.put("日本語", "ok");
            db.flush();
        }
        try (KvStore db = KvStore.open(dir, tiny())) {
            assertTrue(java.util.Arrays.equals(binary, db.get("नमस्ते").get()));
            assertEquals("ok", db.getString("日本語").get());
        }
    }

    @Test
    void dataSurvivesReopenFromTheLogAlone() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, new Options(1 << 20, 4, false))) { // large memtable: nothing flushes
            db.put("a", "1");
            db.put("b", "2");
            db.delete("a");
            assertEquals(0, db.tableCount());
        }
        try (KvStore db = KvStore.open(dir, new Options(1 << 20, 4, false))) {
            assertFalse(db.get("a").isPresent());
            assertEquals("2", db.getString("b").get());
        }
    }

    @Test
    void dataSurvivesReopenFromTables() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, tiny())) {
            for (int i = 0; i < 200; i++) db.put(String.format("key%04d", i), "value" + i);
            assertTrue(db.tableCount() >= 1, "expected flushes to have happened");
        }
        try (KvStore db = KvStore.open(dir, tiny())) {
            for (int i = 0; i < 200; i++) assertEquals("value" + i, db.getString(String.format("key%04d", i)).get());
        }
    }

    @Test
    void deleteShadowsValueInOlderTable() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), new Options(1 << 20, 10, false))) {
            db.put("k", "old");
            db.flush();                       // "old" is now in a table
            db.delete("k");
            assertFalse(db.get("k").isPresent(), "tombstone in memtable must hide the table value");
            db.flush();                       // tombstone is now in a newer table
            assertFalse(db.get("k").isPresent(), "tombstone in a newer table must hide the older table value");
            assertEquals(2, db.tableCount());
        }
    }

    @Test
    void newerTableWinsOverOlder() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), new Options(1 << 20, 10, false))) {
            db.put("k", "v1");
            db.flush();
            db.put("k", "v2");
            db.flush();
            db.put("k", "v3");
            assertEquals("v3", db.getString("k").get());
            db.flush();
            assertEquals("v3", db.getString("k").get());
        }
    }

    @Test
    void scanMergesAllSourcesInOrderAndHidesDeletes() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), new Options(1 << 20, 10, false))) {
            db.put("a", "1"); db.put("c", "1"); db.put("e", "1");
            db.flush();
            db.put("b", "2"); db.put("c", "2"); db.delete("e");
            db.flush();
            db.put("d", "3"); db.put("a", "3");   // still in memtable
            List<Map.Entry<String, byte[]>> all = db.scan(null, null);
            StringBuilder got = new StringBuilder();
            for (var e : all) got.append(e.getKey()).append('=').append(s(e.getValue())).append(' ');
            assertEquals("a=3 b=2 c=2 d=3 ", got.toString());
        }
    }

    @Test
    void scanRespectsBounds() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), tiny())) {
            TreeMap<String, byte[]> model = new TreeMap<>();
            for (int i = 0; i < 100; i++) {
                String k = String.format("k%03d", i);
                db.put(k, "v" + i);
                model.put(k, b("v" + i));
            }
            assertScan(db, model, "k010", "k020");
            assertScan(db, model, null, "k005");
            assertScan(db, model, "k095", null);
            assertScan(db, model, "k050", "k050");   // empty range
            assertScan(db, model, "k060", "k040");   // inverted range
            assertScan(db, model, "zzz", null);      // beyond every key
        }
    }

    private static void assertScan(KvStore db, TreeMap<String, byte[]> model, String from, String to) throws Exception {
        List<String> expected = new ArrayList<>();
        if (from == null && to == null) expected.addAll(model.keySet());
        else if (from == null) expected.addAll(model.headMap(to).keySet());
        else if (to == null) expected.addAll(model.tailMap(from, true).keySet());
        else if (from.compareTo(to) < 0) expected.addAll(model.subMap(from, to).keySet());
        List<String> actual = new ArrayList<>();
        for (var e : db.scan(from, to)) actual.add(e.getKey());
        assertEquals(expected, actual);
    }

    @Test
    void compactionKeepsDataAndDropsGarbage() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, new Options(1 << 20, 100, false))) {
            for (int round = 0; round < 4; round++) {
                for (int i = 0; i < 50; i++) db.put(String.format("k%02d", i), "round" + round);
                db.flush();
            }
            for (int i = 0; i < 50; i += 2) db.delete(String.format("k%02d", i));
            db.flush();
            assertEquals(5, db.tableCount());

            db.compact();
            assertEquals(1, db.tableCount());
            for (int i = 0; i < 50; i++) {
                var v = db.getString(String.format("k%02d", i));
                if (i % 2 == 0) assertFalse(v.isPresent(), "deleted key came back: " + i);
                else assertEquals("round3", v.get());
            }
            // 250 puts + 25 tombstones went in; only the 25 live keys should be on disk.
            long onDisk = SSTable.readAll(dir.resolve(filesIn(dir).stream().filter(f -> f.endsWith(".sst")).findFirst().get())).size();
            assertEquals(25L, onDisk);
        }
    }

    @Test
    void compactingEverythingDeletedLeavesNoTable() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, new Options(1 << 20, 100, false))) {
            db.put("a", "1"); db.put("b", "2");
            db.flush();
            db.delete("a"); db.delete("b");
            db.flush();
            db.compact();
            assertEquals(0, db.tableCount());
            assertFalse(db.get("a").isPresent());
            assertTrue(db.scan(null, null).isEmpty());
        }
        assertTrue(filesIn(dir).stream().noneMatch(f -> f.endsWith(".sst")), "no table files should remain: " + filesIn(dir));
        try (KvStore db = KvStore.open(dir, tiny())) {
            assertTrue(db.scan(null, null).isEmpty());
        }
    }

    @Test
    void automaticCompactionBoundsTableCount() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), tiny())) { // compaction threshold 3
            for (int i = 0; i < 2000; i++) db.put(String.format("key%05d", i), "v" + i);
            assertTrue(db.tableCount() < 3, "tables should be compacted, found " + db.tableCount());
        }
    }

    /**
     * The reason for the per-table Bloom filter: lookups of absent keys should almost never touch disk.
     * The absent keys are built to sit inside the key range of the tables ("t2-key17x" lies between real keys),
     * so only the Bloom filter, not a min/max range check, can reject them.
     */
    @Test
    void bloomFiltersSkipTablesForAbsentKeys() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), new Options(1 << 20, 100, false))) {
            for (int t = 0; t < 4; t++) {
                for (int i = 0; i < 500; i++) db.put("t" + t + "-key" + i, "v");
                db.flush();
            }
            long readsBefore = db.blockReads(), rejectsBefore = db.bloomRejections();
            int probes = 2000;
            for (int i = 0; i < probes; i++) assertFalse(db.get("t" + (i % 4) + "-key" + (i % 500) + "x").isPresent());
            long blockReads = db.blockReads() - readsBefore;
            long rejections = db.bloomRejections() - rejectsBefore;
            long tableChecks = (long) probes * db.tableCount();
            assertTrue(rejections > tableChecks * 0.97, "bloom filters rejected only " + rejections + " of " + tableChecks);
            assertTrue(blockReads < tableChecks * 0.03, "bloom filters let " + blockReads + " of " + tableChecks + " through to disk");
        }
    }

    @Test
    void presentKeysCostAtMostOneBlockReadPerTable() throws Exception {
        try (KvStore db = KvStore.open(tempDir(), new Options(1 << 20, 100, false))) {
            for (int i = 0; i < 1000; i++) db.put(String.format("k%04d", i), "v" + i);
            db.flush();
            long before = db.blockReads();
            for (int i = 0; i < 1000; i++) assertEquals("v" + i, db.getString(String.format("k%04d", i)).get());
            assertEquals(1000L, db.blockReads() - before); // exactly one block read per hit, thanks to the sparse index
        }
    }

    @Test
    void concurrentWritersDoNotLoseData() throws Exception {
        Path dir = tempDir();
        int threads = 8, perThread = 300;
        try (KvStore db = KvStore.open(dir, tiny())) {
            ExecutorService pool = Executors.newFixedThreadPool(threads);
            List<Future<?>> fs = new ArrayList<>();
            for (int t = 0; t < threads; t++) {
                final int id = t;
                fs.add(pool.submit(() -> {
                    for (int i = 0; i < perThread; i++) {
                        db.put("t" + id + "-" + i, "v" + i);
                        if (i % 7 == 0) db.getString("t" + id + "-" + i);
                    }
                    return null;
                }));
            }
            for (Future<?> f : fs) f.get(60, TimeUnit.SECONDS);
            pool.shutdown();
            for (int t = 0; t < threads; t++)
                for (int i = 0; i < perThread; i++) assertEquals("v" + i, db.getString("t" + t + "-" + i).get());
            assertEquals(threads * perThread, db.scan(null, null).size());
        }
        try (KvStore db = KvStore.open(dir, tiny())) {
            assertEquals(threads * perThread, db.scan(null, null).size());
        }
    }

    @Test
    void rejectsBadInputAndUseAfterClose() throws Exception {
        KvStore db = KvStore.open(tempDir(), tiny());
        assertThrows(IllegalArgumentException.class, () -> db.put("", "x"));
        assertThrows(IllegalArgumentException.class, () -> db.put(null, "x"));
        assertThrows(IllegalArgumentException.class, () -> db.put("k", (byte[]) null));
        assertThrows(IllegalArgumentException.class, () -> db.get(""));
        db.close();
        db.close(); // closing twice is harmless
        assertThrows(IllegalStateException.class, () -> db.get("k"));
        assertThrows(IllegalStateException.class, () -> db.put("k", "v"));
    }

    /** Random operations against java.util.TreeMap as the reference, with reopens and explicit flush/compact. */
    @Test
    void randomOperationsMatchTreeMapReference() throws Exception {
        for (long seed = 1; seed <= 5; seed++) {
            Path dir = tempDir();
            java.util.Random rnd = new java.util.Random(seed);
            TreeMap<String, byte[]> model = new TreeMap<>();
            List<String> universe = new ArrayList<>();
            for (int i = 0; i < 60; i++) universe.add(String.format("key%03d", i));
            KvStore db = KvStore.open(dir, tiny());
            for (int step = 0; step < 3000; step++) {
                String key = universe.get(rnd.nextInt(universe.size()));
                int op = rnd.nextInt(100);
                if (op < 55) {
                    byte[] v = b("v" + step);
                    db.put(key, v);
                    model.put(key, v);
                } else if (op < 75) {
                    db.delete(key);
                    model.remove(key);
                } else if (op < 95) {
                    var got = db.get(key);
                    assertEquals(model.containsKey(key), got.isPresent());
                } else if (op < 97) {
                    db.flush();
                } else if (op < 98) {
                    db.compact();
                } else {
                    db.close();
                    db = KvStore.open(dir, tiny());
                }
            }
            assertMatches(db, model, universe);
            db.close();
            try (KvStore reopened = KvStore.open(dir, tiny())) {
                assertMatches(reopened, model, universe);
            }
        }
    }
}
