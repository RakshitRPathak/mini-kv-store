package dev.rakshit.kv;

import static dev.rakshit.kv.TestSupport.*;
import static dev.rakshit.kv.testkit.Assert.*;

import dev.rakshit.kv.testkit.Test;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Write-ahead-log damage and stray-file handling at startup. */
public class RecoveryTests {

    private static final Options BIG = new Options(1 << 20, 10, false); // never flushes: everything lives in the log

    private static long[] recordEnds(Path wal, int n) throws Exception {
        // Replay once to learn record boundaries: each record is 12 header bytes + key + value.
        long[] ends = new long[n];
        long pos = 0;
        List<Long> found = new ArrayList<>();
        Wal.openAndReplay(wal, false, e -> found.add((long) (12 + e.key().length() + (e.value() == null ? 0 : e.value().length)))).close();
        for (int i = 0; i < n; i++) {
            pos += found.get(i);
            ends[i] = pos;
        }
        return ends;
    }

    private static Path writeFiveRecords() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, BIG)) {
            for (int i = 1; i <= 5; i++) db.put("key" + i, "value" + i);
        }
        return dir;
    }

    @Test
    void tornFinalRecordIsDroppedAndEarlierOnesKept() throws Exception {
        Path dir = writeFiveRecords();
        Path wal = dir.resolve("wal.log");
        long[] ends = recordEnds(wal, 5);
        try (RandomAccessFile f = new RandomAccessFile(wal.toFile(), "rw")) {
            f.setLength(ends[4] - 3); // the process died 3 bytes before finishing record 5
        }
        try (KvStore db = KvStore.open(dir, BIG)) {
            for (int i = 1; i <= 4; i++) assertEquals("value" + i, db.getString("key" + i).get());
            assertFalse(db.get("key5").isPresent(), "a half-written record must not appear");
            assertEquals(ends[3], Files.size(wal)); // the torn tail was truncated away
            db.put("key6", "value6");               // and new writes append cleanly after the good data
        }
        try (KvStore db = KvStore.open(dir, BIG)) {
            assertEquals("value6", db.getString("key6").get());
            assertEquals("value4", db.getString("key4").get());
        }
    }

    @Test
    void everyPossibleTruncationPointRecoversAPrefix() throws Exception {
        Path source = writeFiveRecords();
        byte[] full = Files.readAllBytes(source.resolve("wal.log"));
        long[] ends = recordEnds(source.resolve("wal.log"), 5);
        for (int cut = 0; cut <= full.length; cut++) {
            Path dir = tempDir();
            Files.write(dir.resolve("wal.log"), java.util.Arrays.copyOf(full, cut));
            int expectedRecords = 0;
            for (long end : ends) if (end <= cut) expectedRecords++;
            try (KvStore db = KvStore.open(dir, BIG)) {
                assertEquals(expectedRecords, db.scan(null, null).size());
                for (int i = 1; i <= expectedRecords; i++) assertEquals("value" + i, db.getString("key" + i).get());
            }
        }
    }

    @Test
    void flippedBitStopsReplayAtTheCorruptRecord() throws Exception {
        Path dir = writeFiveRecords();
        Path wal = dir.resolve("wal.log");
        long[] ends = recordEnds(wal, 5);
        try (RandomAccessFile f = new RandomAccessFile(wal.toFile(), "rw")) {
            long inRecord3 = ends[1] + 12 + 2;   // a byte inside record 3's key
            f.seek(inRecord3);
            int original = f.read();
            f.seek(inRecord3);
            f.write(original ^ 0x01);
        }
        try (KvStore db = KvStore.open(dir, BIG)) {
            assertEquals("value1", db.getString("key1").get());
            assertEquals("value2", db.getString("key2").get());
            // Records 3, 4 and 5 are dropped: after a bad record we cannot trust anything that follows it.
            assertFalse(db.get("key3").isPresent());
            assertFalse(db.get("key4").isPresent());
            assertFalse(db.get("key5").isPresent());
            assertEquals(ends[1], Files.size(wal));
        }
    }

    @Test
    void trailingGarbageWithHugeLengthFieldsIsIgnoredWithoutHugeAllocation() throws Exception {
        Path dir = writeFiveRecords();
        Path wal = dir.resolve("wal.log");
        byte[] garbage = new byte[64];
        new Random(1).nextBytes(garbage);
        garbage[4] = 0x7f; garbage[5] = (byte) 0xff; garbage[6] = (byte) 0xff; garbage[7] = (byte) 0xff; // keyLen ~2 GB
        Files.write(wal, garbage, StandardOpenOption.APPEND);
        try (KvStore db = KvStore.open(dir, BIG)) {
            assertEquals(5, db.scan(null, null).size());
        }
    }

    @Test
    void replayIsIdempotent() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, BIG)) {
            db.put("a", "1"); db.put("a", "2"); db.delete("b"); db.put("b", "3"); db.delete("b");
        }
        for (int i = 0; i < 5; i++) {
            try (KvStore db = KvStore.open(dir, BIG)) {
                assertEquals("2", db.getString("a").get());
                assertFalse(db.get("b").isPresent());
                assertEquals(1, db.scan(null, null).size());
            }
        }
    }

    @Test
    void openingAnEmptyOrMissingDirectoryWorks() throws Exception {
        Path dir = tempDir().resolve("does").resolve("not").resolve("exist");
        try (KvStore db = KvStore.open(dir, BIG)) {
            assertTrue(db.scan(null, null).isEmpty());
            db.put("x", "y");
        }
        try (KvStore db = KvStore.open(dir, BIG)) {
            assertEquals("y", db.getString("x").get());
        }
    }

    @Test
    void unlistedTableAndTempFilesAreDeletedOnOpen() throws Exception {
        Path dir = tempDir();
        try (KvStore db = KvStore.open(dir, BIG)) {
            db.put("real", "data");
            db.flush();
        }
        // Debris a crash could leave behind: a finished table nobody committed, and half-written temp files.
        Files.write(dir.resolve("table-00000099.sst"), new byte[] {1, 2, 3});
        Files.write(dir.resolve("table-00000100.sst.tmp"), new byte[] {4, 5});
        Files.write(dir.resolve("MANIFEST.tmp"), new byte[] {6});
        try (KvStore db = KvStore.open(dir, BIG)) {
            assertEquals("data", db.getString("real").get());
            assertEquals(1, db.tableCount());
        }
        List<String> files = filesIn(dir);
        assertTrue(files.stream().noneMatch(f -> f.contains("99") || f.contains("100") || f.equals("MANIFEST.tmp")),
                "debris should be gone, found " + files);
    }
}
