package dev.rakshit.kv;

/**
 * Tuning knobs.
 *
 * @param memtableFlushBytes   approximate in-memory size at which the memtable is written out as an SSTable
 * @param compactionThreshold  compact all tables into one once this many exist
 * @param syncWrites           fsync the write-ahead log on every write. {@code true} means an acknowledged
 *                             write survives power loss; {@code false} is much faster but can lose the
 *                             last few writes if the machine (not just the process) dies
 */
public record Options(int memtableFlushBytes, int compactionThreshold, boolean syncWrites) {

    public Options {
        if (memtableFlushBytes <= 0) throw new IllegalArgumentException("memtableFlushBytes must be positive");
        if (compactionThreshold < 2) throw new IllegalArgumentException("compactionThreshold must be at least 2");
    }

    public static Options defaults() {
        return new Options(4 * 1024 * 1024, 4, true);
    }

    public Options withSyncWrites(boolean sync) {
        return new Options(memtableFlushBytes, compactionThreshold, sync);
    }
}
