package dev.rakshit.kv;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;

/**
 * K-way merge of sorted entry streams, newest source first.
 *
 * <p>Classic pattern: a min-heap holding the current head of each stream. Poll the smallest key, advance
 * that stream, push its next head. When several sources hold the same key, only the one from the newest
 * source (lowest rank) is emitted and the older ones are skipped, which is how an overwrite or delete
 * hides the older versions. Cost is O(N log K) for N total entries across K streams.
 *
 * <p>Used for range scans (reading) and for compaction (rewriting several tables as one).
 */
final class MergeIterator implements Iterator<Entry> {

    private record Head(Entry entry, int rank, Iterator<Entry> source) {}

    private final PriorityQueue<Head> heap = new PriorityQueue<>((a, b) -> {
        int c = a.entry().key().compareTo(b.entry().key());
        return c != 0 ? c : Integer.compare(a.rank(), b.rank());
    });
    private final boolean dropTombstones;
    private Entry next;

    /**
     * @param sourcesNewestFirst index 0 is the newest source and wins ties
     * @param dropTombstones     true only when merging <em>every</em> source that could hold the key, so
     *                           nothing older can be hiding underneath the tombstone
     */
    MergeIterator(List<Iterator<Entry>> sourcesNewestFirst, boolean dropTombstones) {
        this.dropTombstones = dropTombstones;
        for (int rank = 0; rank < sourcesNewestFirst.size(); rank++) {
            push(sourcesNewestFirst.get(rank), rank);
        }
        advance();
    }

    private void push(Iterator<Entry> source, int rank) {
        if (source.hasNext()) heap.add(new Head(source.next(), rank, source));
    }

    private void advance() {
        next = null;
        while (!heap.isEmpty()) {
            Head winner = heap.poll();
            push(winner.source(), winner.rank());
            // Discard older versions of the same key.
            while (!heap.isEmpty() && heap.peek().entry().key().equals(winner.entry().key())) {
                Head loser = heap.poll();
                push(loser.source(), loser.rank());
            }
            if (dropTombstones && winner.entry().isTombstone()) continue;
            next = winner.entry();
            return;
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

    /** Convenience for tests. */
    static List<Entry> drain(MergeIterator it) {
        List<Entry> out = new ArrayList<>();
        while (it.hasNext()) out.add(it.next());
        return out;
    }
}
