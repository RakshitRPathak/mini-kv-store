package dev.rakshit.kv;

import static dev.rakshit.kv.testkit.Assert.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;

/** Shared helpers: temp directories (removed on JVM exit), a model to compare against, a crash-injecting hook. */
final class TestSupport {
    private TestSupport() {}

    private static final List<Path> TEMP = new ArrayList<>();

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            for (Path p : TEMP) {
                try (Stream<Path> walk = Files.walk(p)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(f -> f.toFile().delete());
                } catch (IOException ignored) {
                    // best effort cleanup
                }
            }
        }));
    }

    static synchronized Path tempDir() {
        try {
            Path p = Files.createTempDirectory("kv-test-");
            TEMP.add(p);
            return p;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** Small thresholds so tests exercise flush and compaction constantly. */
    static Options tiny() {
        return new Options(300, 3, false);
    }

    static byte[] b(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    static String s(byte[] b) {
        return new String(b, StandardCharsets.UTF_8);
    }

    /** Asserts that the store holds exactly what the model holds, via point lookups and a full scan. */
    static void assertMatches(KvStore store, TreeMap<String, byte[]> model, Collection<String> universe) throws IOException {
        for (String key : universe) {
            byte[] expected = model.get(key);
            var actual = store.get(key);
            if (expected == null) {
                assertFalse(actual.isPresent(), "key '" + key + "' should be absent but has " + actual.map(TestSupport::s).orElse(""));
            } else {
                assertTrue(actual.isPresent(), "key '" + key + "' is missing");
                assertTrue(Arrays.equals(expected, actual.get()), "key '" + key + "' has the wrong value");
            }
        }
        List<Map.Entry<String, byte[]>> scanned = store.scan(null, null);
        assertEquals(model.size(), scanned.size());
        int i = 0;
        for (Map.Entry<String, byte[]> e : model.entrySet()) {
            assertEquals(e.getKey(), scanned.get(i).getKey());
            assertTrue(Arrays.equals(e.getValue(), scanned.get(i).getValue()), "scan value mismatch at " + e.getKey());
            i++;
        }
    }

    /** Throws SimulatedCrash the (skip+1)-th time the armed point is reached. */
    static final class CrashAt implements Hooks {
        private final String point;
        private int skip;
        boolean fired = false;

        CrashAt(String point, int skip) {
            this.point = point;
            this.skip = skip;
        }

        @Override
        public void at(String p) {
            if (fired || !p.equals(point)) return;
            if (skip-- > 0) return;
            fired = true;
            throw new SimulatedCrash(p);
        }
    }

    static List<String> filesIn(Path dir) throws IOException {
        try (Stream<Path> s = Files.list(dir)) {
            return s.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }
}
