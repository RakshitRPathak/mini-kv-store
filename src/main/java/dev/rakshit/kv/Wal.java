package dev.rakshit.kv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.function.Consumer;
import java.util.zip.CRC32;

/**
 * Write-ahead log: every change is appended here and (optionally) fsynced <i>before</i> it is applied
 * to the in-memory table, so a crash can never lose an acknowledged write: on restart the log is replayed.
 *
 * <p>Record layout (big-endian):
 * <pre>
 *   [crc32:int] [keyLen:int] [valLen:int, -1 = delete] [key bytes] [value bytes]
 * </pre>
 * The CRC covers everything after itself. A crash can leave a half-written record at the end ("torn
 * write"), and disks can flip bits. During replay we stop at the first record that is incomplete or fails
 * its CRC, and truncate the file there, so that everything before it is kept and new appends do not land
 * after garbage. Records are replayed in order, so replaying twice gives the same result (idempotent).
 */
final class Wal implements AutoCloseable {

    static final int MAX_KEY_BYTES = 1 << 20;     // 1 MiB
    static final int MAX_VALUE_BYTES = 64 << 20;  // 64 MiB
    private static final int HEADER = 12;

    private final FileChannel channel;
    private final boolean sync;

    private Wal(FileChannel channel, boolean sync) {
        this.channel = channel;
        this.sync = sync;
    }

    /**
     * Opens (creating if needed) the log, replays every intact record into {@code sink}, truncates any torn
     * or corrupt tail, and returns a log positioned for appending.
     */
    static Wal openAndReplay(Path file, boolean sync, Consumer<Entry> sink) throws IOException {
        FileChannel ch = FileChannel.open(file, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long good = 0;
        long size = ch.size();
        ByteBuffer header = ByteBuffer.allocate(HEADER);
        while (true) {
            header.clear();
            if (!readFully(ch, header, good)) break;               // clean end, or torn header
            header.flip();
            int crc = header.getInt();
            int keyLen = header.getInt();
            int valLen = header.getInt();
            if (keyLen < 0 || keyLen > MAX_KEY_BYTES || valLen < -1 || valLen > MAX_VALUE_BYTES) break; // garbage lengths
            int bodyLen = keyLen + Math.max(valLen, 0);
            if (good + HEADER + bodyLen > size) break;              // body was cut short
            ByteBuffer body = ByteBuffer.allocate(bodyLen);
            if (!readFully(ch, body, good + HEADER)) break;
            if (checksum(keyLen, valLen, body.array()) != crc) break; // bit rot or partial overwrite
            byte[] key = new byte[keyLen];
            System.arraycopy(body.array(), 0, key, 0, keyLen);
            byte[] value = null;
            if (valLen >= 0) {
                value = new byte[valLen];
                System.arraycopy(body.array(), keyLen, value, 0, valLen);
            }
            sink.accept(new Entry(new String(key, StandardCharsets.UTF_8), value));
            good += HEADER + bodyLen;
        }
        if (good < size) ch.truncate(good);                         // drop the damaged tail
        ch.position(good);
        return new Wal(ch, sync);
    }

    /** Appends one record; with {@code sync} it is on stable storage when this returns. */
    void append(String key, byte[] value) throws IOException {
        byte[] k = key.getBytes(StandardCharsets.UTF_8);
        int valLen = value == null ? -1 : value.length;
        int bodyLen = k.length + Math.max(valLen, 0);
        byte[] body = new byte[bodyLen];
        System.arraycopy(k, 0, body, 0, k.length);
        if (value != null) System.arraycopy(value, 0, body, k.length, value.length);

        ByteBuffer rec = ByteBuffer.allocate(HEADER + bodyLen);
        rec.putInt(checksum(k.length, valLen, body)).putInt(k.length).putInt(valLen).put(body);
        rec.flip();
        while (rec.hasRemaining()) channel.write(rec);
        if (sync) channel.force(false);
    }

    long sizeBytes() throws IOException {
        return channel.size();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    private static int checksum(int keyLen, int valLen, byte[] body) {
        CRC32 crc = new CRC32();
        ByteBuffer lens = ByteBuffer.allocate(8).putInt(keyLen).putInt(valLen);
        crc.update(lens.array());
        crc.update(body);
        return (int) crc.getValue();
    }

    private static boolean readFully(FileChannel ch, ByteBuffer buf, long position) throws IOException {
        long pos = position;
        while (buf.hasRemaining()) {
            int n = ch.read(buf, pos);
            if (n < 0) return false;
            pos += n;
        }
        return true;
    }
}
