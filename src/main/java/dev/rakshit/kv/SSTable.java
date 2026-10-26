package dev.rakshit.kv;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sorted String Table: an immutable file of entries sorted by key. Once written it is never modified,
 * which makes reads lock-free and crash recovery simple.
 *
 * <pre>
 *   [data]   entries in key order:  [keyLen:int] [valLen:int, -1 = tombstone] [key] [value]
 *   [index]  sparse: the key and file offset of every INDEX_EVERY-th entry
 *   [bloom]  Bloom filter over all keys in this file
 *   [footer] [indexOffset:long] [bloomOffset:long] [entryCount:long] [MAGIC:int]
 * </pre>
 *
 * <p>Point lookup: ask the Bloom filter (memory only); if it says "maybe", binary-search the sparse index
 * for the block that could hold the key and read just that block (at most INDEX_EVERY entries) with one disk read.
 * Keys are ordered by {@link String#compareTo} (UTF-16 code units) everywhere in the store.
 */
final class SSTable {

    static final int INDEX_EVERY = 16;
    private static final int MAGIC = 0x4B565354; // "KVST"
    private static final int FOOTER_BYTES = 8 + 8 + 8 + 4;

    private SSTable() {}

    // ------------------------------------------------------------------ writing

    /**
     * Writes {@code entries}, which must already be in strictly increasing key order, to {@code file}, and
     * fsyncs it. Callers write to a temporary name and rename afterwards so a reader never sees a half-written table.
     *
     * @param expectedCount upper bound on the number of entries, used to size the Bloom filter
     * @return the number of entries written
     */
    static long write(Path file, Iterator<Entry> entries, int expectedCount) throws IOException {
        BloomFilter bloom = new BloomFilter(expectedCount, 0.01);
        List<String> indexKeys = new ArrayList<>();
        List<Long> indexOffsets = new ArrayList<>();
        long count = 0;
        long offset = 0;
        String previous = null;
        try (FileOutputStream fos = new FileOutputStream(file.toFile());
             DataOutputStream out = new DataOutputStream(new BufferedOutputStream(fos, 1 << 16))) {
            while (entries.hasNext()) {
                Entry e = entries.next();
                if (previous != null && previous.compareTo(e.key()) >= 0) {
                    throw new IllegalStateException("entries not strictly sorted: '" + previous + "' then '" + e.key() + "'");
                }
                previous = e.key();
                byte[] k = e.key().getBytes(StandardCharsets.UTF_8);
                int valLen = e.value() == null ? -1 : e.value().length;
                if (count % INDEX_EVERY == 0) {
                    indexKeys.add(e.key());
                    indexOffsets.add(offset);
                }
                out.writeInt(k.length);
                out.writeInt(valLen);
                out.write(k);
                if (valLen > 0) out.write(e.value());
                offset += 8 + k.length + Math.max(valLen, 0);
                bloom.add(e.key());
                count++;
            }
            long indexOffset = offset;
            out.writeInt(indexKeys.size());
            offset += 4;
            for (int i = 0; i < indexKeys.size(); i++) {
                byte[] k = indexKeys.get(i).getBytes(StandardCharsets.UTF_8);
                out.writeInt(k.length);
                out.write(k);
                out.writeLong(indexOffsets.get(i));
                offset += 4 + k.length + 8;
            }
            long bloomOffset = offset;
            bloom.writeTo(out);
            out.writeLong(indexOffset);
            out.writeLong(bloomOffset);
            out.writeLong(count);
            out.writeInt(MAGIC);
            out.flush();
            fos.getFD().sync(); // the file must be durable before anything refers to it
        }
        return count;
    }

    // ------------------------------------------------------------------ reading

    enum State { ABSENT, TOMBSTONE, VALUE }

    record Lookup(State state, byte[] value) {
        static final Lookup ABSENT = new Lookup(State.ABSENT, null);
        static final Lookup TOMBSTONE = new Lookup(State.TOMBSTONE, null);
    }

    static final class Reader implements AutoCloseable {
        private final Path path;
        private final RandomAccessFile raf;
        private final String[] indexKeys;
        private final long[] indexOffsets;
        private final BloomFilter bloom;
        private final long dataEnd;
        private final long entryCount;

        /** Lookups answered "absent" by the Bloom filter alone, with no disk access. */
        final AtomicLong bloomRejections = new AtomicLong();
        /** Lookups that had to read a block from disk. */
        final AtomicLong blockReads = new AtomicLong();

        Reader(Path path) throws IOException {
            this.path = path;
            this.raf = new RandomAccessFile(path.toFile(), "r");
            try {
                long len = raf.length();
                if (len < FOOTER_BYTES) throw new IOException("sstable too short: " + path);
                raf.seek(len - FOOTER_BYTES);
                long indexOffset = raf.readLong();
                long bloomOffset = raf.readLong();
                this.entryCount = raf.readLong();
                if (raf.readInt() != MAGIC) throw new IOException("bad sstable magic: " + path);
                if (indexOffset < 0 || bloomOffset < indexOffset || bloomOffset > len) {
                    throw new IOException("corrupt sstable footer: " + path);
                }
                this.dataEnd = indexOffset;

                byte[] meta = new byte[(int) (len - FOOTER_BYTES - indexOffset)];
                raf.seek(indexOffset);
                raf.readFully(meta);
                DataInputStream in = new DataInputStream(new ByteArrayInputStream(meta));
                int n = in.readInt();
                this.indexKeys = new String[n];
                this.indexOffsets = new long[n];
                for (int i = 0; i < n; i++) {
                    byte[] k = new byte[in.readInt()];
                    in.readFully(k);
                    indexKeys[i] = new String(k, StandardCharsets.UTF_8);
                    indexOffsets[i] = in.readLong();
                }
                this.bloom = BloomFilter.readFrom(in);
            } catch (IOException | RuntimeException e) {
                raf.close();
                throw e;
            }
        }

        Path path() {
            return path;
        }

        long entryCount() {
            return entryCount;
        }

        Lookup get(String key) throws IOException {
            if (indexKeys.length == 0 || !bloom.mightContain(key)) {
                bloomRejections.incrementAndGet();
                return Lookup.ABSENT;
            }
            int block = floorBlock(key);
            if (block < 0) return Lookup.ABSENT; // smaller than every key in the file
            byte[] bytes = readBlock(block);
            DataInputStream in = new DataInputStream(new ByteArrayInputStream(bytes));
            while (in.available() > 0) {
                byte[] k = new byte[in.readInt()];
                int valLen = in.readInt();
                in.readFully(k);
                byte[] v = valLen > 0 ? new byte[valLen] : new byte[0];
                if (valLen > 0) in.readFully(v);
                int cmp = new String(k, StandardCharsets.UTF_8).compareTo(key);
                if (cmp == 0) return valLen < 0 ? Lookup.TOMBSTONE : new Lookup(State.VALUE, v);
                if (cmp > 0) break; // passed where the key would be
            }
            return Lookup.ABSENT;
        }

        /** Entries with {@code from <= key < to} in key order (either bound may be null for unbounded). */
        Iterator<Entry> iterator(String from, String to) throws IOException {
            long start = 0;
            if (from != null && indexKeys.length > 0) {
                int block = floorBlock(from);
                start = block < 0 ? 0 : indexOffsets[block];
            }
            return new SequentialIterator(path, start, dataEnd, from, to);
        }

        /** Index of the last indexed block whose first key is <= key, or -1. */
        private int floorBlock(String key) {
            int lo = 0, hi = indexKeys.length - 1, ans = -1;
            while (lo <= hi) {
                int mid = (lo + hi) >>> 1;
                if (indexKeys[mid].compareTo(key) <= 0) {
                    ans = mid;
                    lo = mid + 1;
                } else {
                    hi = mid - 1;
                }
            }
            return ans;
        }

        private byte[] readBlock(int block) throws IOException {
            long start = indexOffsets[block];
            long end = block + 1 < indexOffsets.length ? indexOffsets[block + 1] : dataEnd;
            byte[] bytes = new byte[(int) (end - start)];
            synchronized (raf) { // seek + read must not interleave between threads
                raf.seek(start);
                raf.readFully(bytes);
            }
            blockReads.incrementAndGet();
            return bytes;
        }

        @Override
        public void close() throws IOException {
            raf.close();
        }
    }

    /** Streams entries from {@code start} up to {@code end}, filtered to [from, to). Opens its own file handle. */
    private static final class SequentialIterator implements Iterator<Entry>, AutoCloseable {
        private final DataInputStream in;
        private final long end;
        private long position;
        private final String to;
        private final String from;
        private Entry next;
        private boolean done;

        SequentialIterator(Path path, long start, long end, String from, String to) throws IOException {
            FileInputStream fis = new FileInputStream(path.toFile());
            long skipped = fis.skip(start);
            if (skipped != start) {
                fis.close();
                throw new IOException("could not seek to " + start + " in " + path);
            }
            this.in = new DataInputStream(new BufferedInputStream(fis, 1 << 16));
            this.position = start;
            this.end = end;
            this.from = from;
            this.to = to;
            advance();
        }

        private void advance() {
            try {
                while (position < end) {
                    int keyLen = in.readInt();
                    int valLen = in.readInt();
                    byte[] k = new byte[keyLen];
                    in.readFully(k);
                    byte[] v = null;
                    if (valLen >= 0) {
                        v = new byte[valLen];
                        in.readFully(v);
                    }
                    position += 8 + keyLen + Math.max(valLen, 0);
                    String key = new String(k, StandardCharsets.UTF_8);
                    if (from != null && key.compareTo(from) < 0) continue;
                    if (to != null && key.compareTo(to) >= 0) break;
                    next = new Entry(key, v);
                    return;
                }
                next = null;
                finish();
            } catch (IOException e) {
                finish();
                throw new java.io.UncheckedIOException(e);
            }
        }

        private void finish() {
            if (done) return;
            done = true;
            try {
                in.close();
            } catch (IOException ignored) {
                // nothing useful to do
            }
        }

        @Override
        public boolean hasNext() {
            return next != null;
        }

        @Override
        public Entry next() {
            if (next == null) throw new NoSuchElementException();
            Entry e = next;
            advance();
            return e;
        }

        @Override
        public void close() {
            finish();
        }
    }

    /** Helper for tests and tools: reads an entire table into a list. */
    static List<Entry> readAll(Path file) throws IOException {
        try (Reader r = new Reader(file)) {
            List<Entry> out = new ArrayList<>();
            Iterator<Entry> it = r.iterator(null, null);
            while (it.hasNext()) out.add(it.next());
            return out;
        }
    }
}
