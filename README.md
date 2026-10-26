# mini-kv-store

[![build-and-test](https://github.com/RakshitRPathak/mini-kv-store/actions/workflows/ci.yml/badge.svg)](https://github.com/RakshitRPathak/mini-kv-store/actions/workflows/ci.yml)

A small persistent key-value store in Java, built the way LSM-tree databases are (LevelDB, RocksDB,
Cassandra, and the storage layer behind systems like DynamoDB): an append-only log, a sorted in-memory
table, immutable sorted files on disk, Bloom filters, and compaction. About 1,000 lines, no dependencies.

```bash
./build.sh                                          # needs only a JDK 17+: compiles and runs all 39 tests
java -cp out/main:out/test dev.rakshit.kv.Bench     # throughput numbers (after build.sh)
```

```java
try (KvStore db = KvStore.open(Path.of("data"))) {
    db.put("user:42", "Ada");
    db.getString("user:42");              // Optional["Ada"]
    db.delete("user:42");
    db.scan("user:", "user;");            // sorted range query across memory and disk
}
```

## How it works

```
 put/delete ──► [write-ahead log]──► [memtable: sorted map in RAM] ──flush──► [SSTable files] ──compaction──► one SSTable
                    (on disk)                                                    (immutable, sorted,
 get ──► memtable ──► SSTables newest → oldest (Bloom filter skips most)         sparse index + Bloom filter)
```

* **Write-ahead log** (`Wal`): each write is appended with a CRC and fsynced before it is applied, so an
  acknowledged write survives a crash. On restart the log is replayed; a half-written final record or a
  corrupted record ends the replay and the damaged tail is cut off.
* **Memtable**: a `TreeMap` that absorbs writes. When it reaches a size limit it is written out as a table.
* **SSTable** (`SSTable`): an immutable file of entries in key order, plus a sparse index (one key per 16
  entries) and a Bloom filter. A point lookup is one memory check, one binary search and at most one disk read.
* **Tombstones**: a delete writes a marker that hides older values; compaction removes both.
* **Compaction** (`MergeIterator`): a k-way merge (min-heap) of all tables, newest value wins, tombstones
  dropped, written as a single new table.
* **MANIFEST**: the list of live tables, replaced by atomic rename. This is what makes flush and compaction
  crash-safe (see below).

## Crash safety, and how it is tested

Flush and compaction are each several steps. The store orders them so that stopping after any step leaves a
recoverable state, and `Hooks` marks each step so tests can "kill the process" there:

| Step in flush | If the process dies here |
|---|---|
| table written under a `.tmp` name | unlisted file; deleted at startup; the log still has the data |
| table renamed into place | same: not in the manifest, so ignored and deleted; log has the data |
| **manifest replaced (commit point)** | table is live; the log replays the same data on top (harmless) |
| log discarded | done |

Compaction follows the same pattern: write the merged table, replace the manifest in one atomic rename, then
delete the old tables.

The tests are designed so that real bugs make them fail:

* **Crash injection**: a random workload runs with a crash armed at each of 8 points in flush and compaction
  (36 runs per point). After the crash the directory is reopened and compared with a `TreeMap` model; the
  recovered store is then used further and compared again.
* **Log damage**: the log is truncated at *every* byte offset and each time exactly the complete records
  come back; flipped bits stop replay at the bad record; garbage with a ~2 GB length field is ignored without
  allocating it.
* **Why a manifest**: after compaction commits, a crash can leave *some* of the now-garbage old tables on disk
  (file deletions are not guaranteed to become permanent in order). The test deletes the tombstone's table but
  leaves the table holding the old value, and checks the deleted key stays deleted.
* **Mutation checks** (run by hand, not part of `build.sh`): I broke the store deliberately in three ways and
  confirmed the suite fails each time: loading every `.sst` file instead of trusting the manifest (3 tests fail,
  including the resurrection one), discarding the log before the manifest commit (2 fail), and not truncating a
  damaged log tail (2 fail).
* Plus: a random-operation comparison against `TreeMap` with reopens, flushes and compactions; 8 concurrent
  writer threads; Bloom filter behaviour; unsorted or truncated table files are rejected.

## Measured performance

From `Bench`, on a 2-core cloud sandbox (ext4). Your numbers will differ, mainly the fsync line, which depends
on the disk. Three runs each; the range is shown.

| Operation | Throughput |
|---|---|
| put, fsync off (100-byte values, 200,000 puts) | 113,000 – 138,000 ops/s |
| put, fsync on (every write durable) | 5,700 – 6,700 ops/s |
| get, key present (random) | 130,000 – 169,000 ops/s |
| get, key absent | ~500,000 ops/s |

For absent keys, 296,892 of 300,000 table checks (99.0%) were answered by the Bloom filters alone and only
984 (0.3%) read a block from disk. The ~20x gap between fsync on and off is the price of surviving power
loss, and is why real systems batch writes ("group commit").

## Limitations (what a production engine adds)

* One lock: operations are thread-safe but run one at a time. Real engines flush and compact in the background.
* Full compaction only: it rewrites everything. Leveled or size-tiered compaction bounds that cost.
* The log is checksummed but SSTable data blocks are not; no compression; no snapshots or transactions.
* Whole values are held in memory; keys are limited to 1 MiB and values to 64 MiB.
* Ordering is Java `String.compareTo` (UTF-16 code units), not byte order.

## Layout

```
src/main/java/dev/rakshit/kv/   KvStore, Wal, SSTable, MergeIterator, BloomFilter, Options, Hooks, Entry
src/test/java/dev/rakshit/kv/   BasicTests, RecoveryTests, CrashInjectionTests, ComponentTests, Bench, testkit/
```
