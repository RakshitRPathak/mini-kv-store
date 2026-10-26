package dev.rakshit.kv;

/**
 * One key with its value. A {@code null} value is a <b>tombstone</b>: a marker that the key was deleted.
 *
 * <p>Tombstones are needed because data is immutable once written to disk. Deleting a key cannot erase it
 * from an older file, so we write a marker that shadows the old value until compaction removes both.
 */
record Entry(String key, byte[] value) {
    boolean isTombstone() {
        return value == null;
    }
}
