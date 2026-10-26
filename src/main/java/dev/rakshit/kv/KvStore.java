package dev.rakshit.kv;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

/**
 * A small persistent key-value store built the way LSM-tree databases (LevelDB, RocksDB, Cassandra,
 * DynamoDB's storage nodes) are:
 *
 * <pre>
 *   put/delete --> [WAL on disk] --> [memtable in RAM] --flush--> [SSTable files] --compaction--> fewer, bigger SSTables
 *   get        --> memtable, then SSTables newest to oldest (Bloom filter skips files)
 * </pre>
 *
 * <h2>Why it is built this way</h2>
 * Random in-place disk updates are slow, so every write is an append: first to the log (for durability), then
 * to a sorted in-memory table. When that table grows large it is written out sequentially as an immutable sorted
 * file. Reads check newest data first. Compaction merges files so reads do not slow down as files pile up.
 *
 * <h2>Crash safety (the interesting part)</h2>
 * Every multi-step change is ordered so that stopping after <em>any</em> step leaves a recoverable state:
 * <ol>
 *   <li><b>Write path.</b> Log first, memory second. If we die between them, replaying the log restores the write.</li>
 *   <li><b>Flush.</b> Write the new table under a temporary name and fsync it, rename it into place, then atomically
 *       replace the MANIFEST (the list of live tables), and only then discard the log. A crash before the manifest
 *       update leaves an unlisted file that startup deletes, with the log still holding the data. A crash after it
 *       leaves a log that replays harmlessly onto data already in a table.</li>
 *   <li><b>Compaction.</b> Same shape: write merged table, then swap the manifest in one atomic rename, then delete
 *       the old tables. The manifest matters here: the merged table has dropped the tombstones, and after a crash an
 *       arbitrary subset of the old tables may still be on disk (a filesystem does not make deletions permanent in
 *       the order they were issued). If "live tables" meant "whatever files exist", a survivor holding an old value
 *       whose tombstone's table was already unlinked would bring a deleted key back to life.</li>
 * </ol>
 * The {@link Hooks} calls mark each of those steps so tests can crash the store at every one of them.
 *
 * <h2>Concurrency</h2>
 * All public methods take one lock, so the store is safe to share between threads but only one operation runs at a
 * time. Real engines use finer locking and background flush/compaction threads.
 *
 * <h2>Limits</h2>
 * Keys are strings, values are byte arrays. Full compaction only (all tables into one), which is simple but rewrites
 * everything; leveled or size-tiered compaction would bound that cost. No snapshots, no checksums on SSTable data
 * blocks (the log is checksummed).
 */
public final class KvStore implements AutoCloseable {

    private static final String WAL_FILE = "wal.log";
    private static final String MANIFEST = "MANIFEST";

    private final Path dir;
    private final Options options;
    private final Hooks hooks;
    private final TreeMap<String, byte[]> memtable = new TreeMap<>(); // null value = tombstone
    private long memtableBytes = 0;
    private final List<SSTable.Reader> tables = new ArrayList<>(); // oldest first
    private Wal wal;
    private long nextTableId = 1;
    private boolean closed = false;

    // ------------------------------------------------------------------ opening and recovery

    public static KvStore open(Path dir) throws IOException {
        return open(dir, Options.defaults());
    }

    public static KvStore open(Path dir, Options options) throws IOException {
        return open(dir, options, Hooks.NONE);
    }

    static KvStore open(Path dir, Options options, Hooks hooks) throws IOException {
        KvStore s = new KvStore(dir, options, hooks);
        s.recover();
        return s;
    }

    private KvStore(Path dir, Options options, Hooks hooks) {
        this.dir = dir;
        this.options = options;
        this.hooks = hooks;
    }

    private void recover() throws IOException {
        Files.createDirectories(dir);

        // 1. The manifest is the only source of truth for which tables are live.
        List<String> live = readManifest();

        // 2. Anything else that looks like ours is debris from an interrupted flush/compaction: delete it.
        Set<String> liveSet = new HashSet<>(live);
        try (DirectoryStream<Path> files = Files.newDirectoryStream(dir)) {
            for (Path f : files) {
                String name = f.getFileName().toString();
                boolean ours = name.endsWith(".sst") || name.endsWith(".tmp");
                if (ours && !liveSet.contains(name)) Files.deleteIfExists(f);
            }
        }

        // 3. Open the live tables.
        for (String name : live) {
            tables.add(new SSTable.Reader(dir.resolve(name)));
            nextTableId = Math.max(nextTableId, tableId(name) + 1);
        }

        // 4. Replay the log into the memtable. Entries already flushed to a table are simply re-applied.
        wal = Wal.openAndReplay(dir.resolve(WAL_FILE), options.syncWrites(), e -> applyToMemtable(e.key(), e.value()));
    }

    // ------------------------------------------------------------------ public API

    public synchronized void put(String key, byte[] value) throws IOException {
        if (value == null) throw new IllegalArgumentException("value must not be null (use delete)");
        if (value.length > Wal.MAX_VALUE_BYTES) throw new IllegalArgumentException("value too large");
        write(key, value);
    }

    public void put(String key, String value) throws IOException {
        put(key, value.getBytes(StandardCharsets.UTF_8));
    }

    public synchronized void delete(String key) throws IOException {
        write(key, null);
    }

    public synchronized Optional<byte[]> get(String key) throws IOException {
        ensureOpen();
        requireKey(key);
        if (memtable.containsKey(key)) return Optional.ofNullable(memtable.get(key)); // null = deleted
        for (int i = tables.size() - 1; i >= 0; i--) { // newest first
            SSTable.Lookup r = tables.get(i).get(key);
            if (r.state() == SSTable.State.VALUE) return Optional.of(r.value());
            if (r.state() == SSTable.State.TOMBSTONE) return Optional.empty();
        }
        return Optional.empty();
    }

    public Optional<String> getString(String key) throws IOException {
        return get(key).map(b -> new String(b, StandardCharsets.UTF_8));
    }

    /** Live entries with {@code from <= key < to}, in key order. Pass null for an open end. */
    public synchronized List<Map.Entry<String, byte[]>> scan(String from, String to) throws IOException {
        ensureOpen();
        List<Iterator<Entry>> sources = new ArrayList<>();
        Map<String, byte[]> slice = from == null && to == null ? memtable
                : from == null ? memtable.headMap(to)
                : to == null ? memtable.tailMap(from, true)
                : from.compareTo(to) >= 0 ? Collections.<String, byte[]>emptyMap() : memtable.subMap(from, to);
        List<Entry> mem = new ArrayList<>();
        for (Map.Entry<String, byte[]> e : slice.entrySet()) mem.add(new Entry(e.getKey(), e.getValue()));
        sources.add(mem.iterator());
        for (int i = tables.size() - 1; i >= 0; i--) sources.add(tables.get(i).iterator(from, to));

        List<Map.Entry<String, byte[]>> out = new ArrayList<>();
        MergeIterator merged = new MergeIterator(sources, true);
        while (merged.hasNext()) {
            Entry e = merged.next();
            out.add(Map.entry(e.key(), e.value()));
        }
        return out;
    }

    /** Forces the memtable to disk as a new SSTable. */
    public synchronized void flush() throws IOException {
        ensureOpen();
        flushLocked();
    }

    /** Merges every SSTable into one, dropping overwritten values and deleted keys. */
    public synchronized void compact() throws IOException {
        ensureOpen();
        compactLocked();
    }

    public synchronized int tableCount() {
        return tables.size();
    }

    /** Number of SSTable lookups answered by a Bloom filter without touching disk. */
    public synchronized long bloomRejections() {
        long n = 0;
        for (SSTable.Reader t : tables) n += t.bloomRejections.get();
        return n;
    }

    /** Number of SSTable blocks actually read from disk by point lookups. */
    public synchronized long blockReads() {
        long n = 0;
        for (SSTable.Reader t : tables) n += t.blockReads.get();
        return n;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) return;
        closed = true;
        // Data in the memtable is already safe in the log, so closing does not need to flush.
        IOException first = null;
        try {
            wal.close();
        } catch (IOException e) {
            first = e;
        }
        for (SSTable.Reader t : tables) {
            try {
                t.close();
            } catch (IOException e) {
                if (first == null) first = e;
            }
        }
        if (first != null) throw first;
    }

    /** Test support: release file handles as if the process died, without any orderly shutdown work. */
    synchronized void abandon() {
        closed = true;
        try {
            wal.close();
        } catch (IOException ignored) {
            // simulating a crash: nothing to do
        }
        for (SSTable.Reader t : tables) {
            try {
                t.close();
            } catch (IOException ignored) {
                // simulating a crash: nothing to do
            }
        }
    }

    // ------------------------------------------------------------------ write path

    private void write(String key, byte[] value) throws IOException {
        ensureOpen();
        requireKey(key);
        wal.append(key, value);                 // 1. durable first
        applyToMemtable(key, value);            // 2. then visible
        if (memtableBytes >= options.memtableFlushBytes()) flushLocked();
    }

    private void applyToMemtable(String key, byte[] value) {
        byte[] old = memtable.put(key, value);
        memtableBytes += 16 + key.length() + (value == null ? 0 : value.length);
        if (old != null) memtableBytes -= 16 + key.length() + old.length;
    }

    private void flushLocked() throws IOException {
        if (memtable.isEmpty()) return;
        List<Entry> entries = new ArrayList<>(memtable.size());
        for (Map.Entry<String, byte[]> e : memtable.entrySet()) entries.add(new Entry(e.getKey(), e.getValue()));

        String name = newTableName();
        Path tmp = dir.resolve(name + ".tmp");
        Path finalPath = dir.resolve(name);

        SSTable.write(tmp, entries.iterator(), entries.size());
        hooks.at("flush.afterTableWritten");             // table exists only under a .tmp name
        Files.move(tmp, finalPath, StandardCopyOption.ATOMIC_MOVE);
        hooks.at("flush.afterTableRenamed");             // table is on disk but not yet in the manifest

        List<String> names = liveNames();
        names.add(name);
        writeManifest(names);                            // the commit point
        hooks.at("flush.afterManifest");                 // table is live; log still holds the same data

        tables.add(new SSTable.Reader(finalPath));
        resetWal();
        memtable.clear();
        memtableBytes = 0;
        hooks.at("flush.done");

        if (tables.size() >= options.compactionThreshold()) compactLocked();
    }

    private void compactLocked() throws IOException {
        if (tables.size() < 2) return;
        List<Iterator<Entry>> sources = new ArrayList<>();
        int upperBound = 0;
        for (int i = tables.size() - 1; i >= 0; i--) { // newest first so newer values win
            sources.add(tables.get(i).iterator(null, null));
            upperBound += (int) tables.get(i).entryCount();
        }
        // Dropping tombstones is safe here because we merge ALL tables: nothing older can be hiding below them.
        // The memtable is not merged, but it is newer than every table so it still shadows them correctly.
        MergeIterator merged = new MergeIterator(sources, true);

        String name = newTableName();
        Path tmp = dir.resolve(name + ".tmp");
        Path finalPath = dir.resolve(name);
        boolean empty = SSTable.write(tmp, merged, Math.max(1, upperBound)) == 0;
        hooks.at("compact.afterTableWritten");
        List<String> newNames = new ArrayList<>();
        if (empty) {
            Files.deleteIfExists(tmp);                   // everything was deleted: no output table needed
        } else {
            Files.move(tmp, finalPath, StandardCopyOption.ATOMIC_MOVE);
            newNames.add(name);
        }
        hooks.at("compact.afterTableRenamed");           // merged table on disk, manifest still lists the old tables

        writeManifest(newNames);                         // the commit point: one atomic rename swaps old for new
        hooks.at("compact.afterManifest");               // old tables are now garbage but still present

        List<SSTable.Reader> old = new ArrayList<>(tables);
        tables.clear();
        if (!empty) tables.add(new SSTable.Reader(finalPath));
        for (SSTable.Reader t : old) {
            t.close();
            Files.deleteIfExists(t.path());
        }
        hooks.at("compact.done");
    }

    // ------------------------------------------------------------------ manifest and files

    private List<String> liveNames() {
        List<String> names = new ArrayList<>();
        for (SSTable.Reader t : tables) names.add(t.path().getFileName().toString());
        return names;
    }

    private List<String> readManifest() throws IOException {
        Path m = dir.resolve(MANIFEST);
        if (!Files.exists(m)) return new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (String line : Files.readAllLines(m, StandardCharsets.UTF_8)) {
            if (!line.isBlank()) names.add(line.trim());
        }
        return names;
    }

    /** Replaces the manifest atomically: write a temp file, fsync it, rename over the old one. */
    private void writeManifest(List<String> names) throws IOException {
        Path tmp = dir.resolve(MANIFEST + ".tmp");
        byte[] content = String.join("\n", names).concat(names.isEmpty() ? "" : "\n").getBytes(StandardCharsets.UTF_8);
        try (var ch = java.nio.channels.FileChannel.open(tmp, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING,
                StandardOpenOption.WRITE)) {
            ch.write(java.nio.ByteBuffer.wrap(content));
            ch.force(true);
        }
        try {
            Files.move(tmp, dir.resolve(MANIFEST), StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            throw new IOException("filesystem does not support atomic rename; refusing to risk a torn manifest", e);
        }
    }

    private void resetWal() throws IOException {
        wal.close();
        Files.deleteIfExists(dir.resolve(WAL_FILE));
        wal = Wal.openAndReplay(dir.resolve(WAL_FILE), options.syncWrites(), e -> {});
    }

    private String newTableName() {
        return String.format("table-%08d.sst", nextTableId++);
    }

    private static long tableId(String name) {
        return Long.parseLong(name.substring("table-".length(), name.length() - ".sst".length()));
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("store is closed");
    }

    private static void requireKey(String key) {
        if (key == null || key.isEmpty()) throw new IllegalArgumentException("key must be non-empty");
        if (key.getBytes(StandardCharsets.UTF_8).length > Wal.MAX_KEY_BYTES) throw new IllegalArgumentException("key too large");
    }
}
