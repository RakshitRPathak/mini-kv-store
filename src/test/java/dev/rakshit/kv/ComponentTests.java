package dev.rakshit.kv;

import static dev.rakshit.kv.TestSupport.*;
import static dev.rakshit.kv.testkit.Assert.*;

import dev.rakshit.kv.testkit.Test;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** The building blocks on their own: SSTable, MergeIterator, Bloom filter, Wal. */
public class ComponentTests {

    private static Entry e(String k, String v) {
        return new Entry(k, v == null ? null : b(v));
    }

    private static Path table(Entry... entries) throws Exception {
        Path f = tempDir().resolve("t.sst");
        SSTable.write(f, List.of(entries).iterator(), entries.length);
        return f;
    }

    private static String render(List<Entry> es) {
        StringBuilder sb = new StringBuilder();
        for (Entry x : es) sb.append(x.key()).append('=').append(x.isTombstone() ? "<del>" : s(x.value())).append(' ');
        return sb.toString().trim();
    }

    // ---------------------------------------------------------------- SSTable

    @Test
    void sstableFindsEveryKeyAcrossBlockBoundaries() throws Exception {
        int n = SSTable.INDEX_EVERY * 5 + 3; // several full blocks and a partial last one
        Entry[] es = new Entry[n];
        for (int i = 0; i < n; i++) es[i] = e(String.format("k%04d", i * 2), "v" + i); // even numbers only
        try (SSTable.Reader r = new SSTable.Reader(table(es))) {
            assertEquals((long) n, r.entryCount());
            for (int i = 0; i < n; i++) {
                SSTable.Lookup l = r.get(String.format("k%04d", i * 2));
                assertEquals(SSTable.State.VALUE, l.state());
                assertEquals("v" + i, s(l.value()));
            }
            // odd keys fall between stored keys, plus one below the first and one above the last
            for (int i = 0; i < n; i++) assertEquals(SSTable.State.ABSENT, r.get(String.format("k%04d", i * 2 + 1)).state());
            assertEquals(SSTable.State.ABSENT, r.get("a").state());
            assertEquals(SSTable.State.ABSENT, r.get("zzzz").state());
        }
    }

    @Test
    void sstableDistinguishesTombstoneEmptyValueAndAbsent() throws Exception {
        try (SSTable.Reader r = new SSTable.Reader(table(e("a", null), e("b", ""), e("c", "x")))) {
            assertEquals(SSTable.State.TOMBSTONE, r.get("a").state());
            assertEquals(SSTable.State.VALUE, r.get("b").state());
            assertEquals(0, r.get("b").value().length);
            assertEquals(SSTable.State.ABSENT, r.get("bb").state());
        }
    }

    @Test
    void sstableIteratorHonoursRanges() throws Exception {
        Entry[] es = new Entry[100];
        for (int i = 0; i < 100; i++) es[i] = e(String.format("k%03d", i), "v" + i);
        try (SSTable.Reader r = new SSTable.Reader(table(es))) {
            assertEquals(100, drain(r.iterator(null, null)).size());
            List<Entry> mid = drain(r.iterator("k030", "k040"));
            assertEquals(10, mid.size());
            assertEquals("k030", mid.get(0).key());
            assertEquals("k039", mid.get(9).key());
            assertEquals(5, drain(r.iterator("k095", null)).size());
            assertEquals(3, drain(r.iterator(null, "k003")).size());
            assertEquals(0, drain(r.iterator("k050", "k050")).size());
            assertEquals(0, drain(r.iterator("zzz", null)).size());
            assertEquals(1, drain(r.iterator("k0305", "k032")).size()); // range starts between keys: only k031 qualifies
        }
    }

    private static List<Entry> drain(Iterator<Entry> it) {
        List<Entry> out = new ArrayList<>();
        while (it.hasNext()) out.add(it.next());
        return out;
    }

    @Test
    void sstableRefusesUnsortedInput() throws Exception {
        Path f = tempDir().resolve("bad.sst");
        assertThrows(IllegalStateException.class,
                () -> SSTable.write(f, List.of(e("b", "1"), e("a", "2")).iterator(), 2));
        assertThrows(IllegalStateException.class,
                () -> SSTable.write(f, List.of(e("a", "1"), e("a", "2")).iterator(), 2)); // duplicates too
    }

    @Test
    void sstableRejectsATruncatedFile() throws Exception {
        Path f = table(e("a", "1"), e("b", "2"));
        byte[] bytes = java.nio.file.Files.readAllBytes(f);
        java.nio.file.Files.write(f, java.util.Arrays.copyOf(bytes, bytes.length - 5));
        assertThrows(java.io.IOException.class, () -> new SSTable.Reader(f).close());
    }

    // ---------------------------------------------------------------- MergeIterator

    @Test
    void mergeEmitsSortedKeysWithNewestSourceWinning() {
        List<Entry> newest = List.of(e("b", "new"), e("d", "new"));
        List<Entry> middle = List.of(e("a", "mid"), e("b", "mid"), e("e", "mid"));
        List<Entry> oldest = List.of(e("a", "old"), e("c", "old"), e("d", "old"));
        MergeIterator it = new MergeIterator(List.of(newest.iterator(), middle.iterator(), oldest.iterator()), false);
        assertEquals("a=mid b=new c=old d=new e=mid", render(MergeIterator.drain(it)));
    }

    @Test
    void mergeTombstonesHideOlderValuesAndCanBeDropped() {
        List<Entry> newer = List.of(e("a", null), e("c", "x"));
        List<Entry> older = List.of(e("a", "old"), e("b", "old"));
        assertEquals("a=<del> b=old c=x",
                render(MergeIterator.drain(new MergeIterator(List.of(newer.iterator(), older.iterator()), false))));
        assertEquals("b=old c=x",
                render(MergeIterator.drain(new MergeIterator(List.of(newer.iterator(), older.iterator()), true))));
    }

    @Test
    void mergeHandlesEmptySources() {
        MergeIterator it = new MergeIterator(
                List.of(new ArrayList<Entry>().iterator(), List.of(e("a", "1")).iterator(), new ArrayList<Entry>().iterator()), false);
        assertEquals("a=1", render(MergeIterator.drain(it)));
        assertFalse(new MergeIterator(List.of(), false).hasNext());
    }

    // ---------------------------------------------------------------- Bloom filter

    @Test
    void bloomSurvivesSerialisationAndKeepsItsGuarantees() throws Exception {
        BloomFilter bf = new BloomFilter(5000, 0.01);
        for (int i = 0; i < 5000; i++) bf.add("in-" + i);
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        bf.writeTo(new DataOutputStream(buf));
        BloomFilter loaded = BloomFilter.readFrom(new DataInputStream(new ByteArrayInputStream(buf.toByteArray())));
        assertEquals(bf.bitCount(), loaded.bitCount());
        assertEquals(bf.hashCount(), loaded.hashCount());
        for (int i = 0; i < 5000; i++) assertTrue(loaded.mightContain("in-" + i), "false negative for in-" + i);
        int fp = 0;
        for (int i = 0; i < 50_000; i++) if (loaded.mightContain("out-" + i)) fp++;
        assertInRange(fp / 50_000.0, 0.002, 0.02, "false-positive rate after reload");
    }

    // ---------------------------------------------------------------- Wal

    @Test
    void walRoundTripsPutsDeletesAndEmptyValues() throws Exception {
        Path f = tempDir().resolve("wal.log");
        try (Wal w = Wal.openAndReplay(f, true, x -> {})) {
            w.append("a", b("1"));
            w.append("b", null);
            w.append("c", new byte[0]);
            w.append("日本", b("ü"));
        }
        List<Entry> seen = new ArrayList<>();
        Wal.openAndReplay(f, false, seen::add).close();
        assertEquals("a=1 b=<del> c= 日本=ü", render(seen));
        assertTrue(seen.get(2).value() != null && seen.get(2).value().length == 0, "empty value must not become a tombstone");
    }
}
