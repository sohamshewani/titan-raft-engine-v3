package com.titan.raft.storage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;

public final class WALSegment implements AutoCloseable {

    public static final int SEGMENT_SIZE = 64 * 1024 * 1024; // 64 MB
    public static final int HEADER_SIZE = 32;
    public static final int MAGIC_NUMBER = 0x54495441; // "TITA"

    // Header layout offsets within the 32-byte block:
    // 0-3:   magic (int)
    // 4-7:   crc32c (int)
    // 8-15:  term (long)
    // 16-23: index (long)
    // 24-31: payload_length (long)

    private final long baseIndex;
    private final File file;
    private final RandomAccessFile raf;
    private final FileChannel channel;
    private final MemorySegment mappedSegment;
    private final Arena arena;
    
    private static final VarHandle INT_HANDLE = MemorySegment.JAVA_INT.varHandle();
    private static final VarHandle LONG_HANDLE = MemorySegment.JAVA_LONG.varHandle();

    private long writeOffset;
    private boolean closed;

    public WALSegment(File file, long baseIndex) throws IOException {
        this.file = file;
        this.baseIndex = baseIndex;
        this.arena = Arena.ofShared();
        
        boolean exists = file.exists();
        this.raf = new RandomAccessFile(file, "rw");
        this.raf.setLength(SEGMENT_SIZE);
        this.channel = raf.getChannel();
        
        this.mappedSegment = channel.map(FileChannel.MapMode.READ_WRITE, 0, SEGMENT_SIZE, arena);
        this.writeOffset = 0;
        
        if (exists) {
            recoverAndValidate();
        }
    }

    public synchronized long append(long term, long index, byte[] payload) throws IOException {
        ensureOpen();
        int payloadLen = payload.length;
        int totalEntrySize = HEADER_SIZE + payloadLen;
        
        if (writeOffset + totalEntrySize > SEGMENT_SIZE) {
            throw new IOException("WALSegment capacity exceeded. Max size: " + SEGMENT_SIZE + ", required: " + (writeOffset + totalEntrySize));
        }

        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payloadLen);
        int checksum = (int) crc32c.getValue();

        long currentOffset = writeOffset;

        // Write header
        mappedSegment.set(INT_HANDLE, currentOffset, MAGIC_NUMBER);
        mappedSegment.set(INT_HANDLE, currentOffset + 4, checksum);
        mappedSegment.set(LONG_HANDLE, currentOffset + 8, term);
        mappedSegment.set(LONG_HANDLE, currentOffset + 16, index);
        mappedSegment.set(LONG_HANDLE, currentOffset + 24, (long) payloadLen);

        // Write payload
        MemorySegment payloadSlice = mappedSegment.asSlice(currentOffset + HEADER_SIZE, payloadLen);
        payloadSlice.copyFrom(MemorySegment.ofArray(payload));

        writeOffset += totalEntrySize;
        return currentOffset;
    }

    public synchronized LogEntry read(long offset) throws IOException {
        ensureOpen();
        if (offset < 0 || offset + HEADER_SIZE > writeOffset) {
            throw new IllegalArgumentException("Invalid read offset: " + offset);
        }

        int magic = (int) mappedSegment.get(INT_HANDLE, offset);
        if (magic != MAGIC_NUMBER) {
            throw new IOException("Corrupted log entry: invalid magic number at offset " + offset);
        }

        int storedChecksum = (int) mappedSegment.get(INT_HANDLE, offset + 4);
        long term = (long) mappedSegment.get(LONG_HANDLE, offset + 8);
        long index = (long) mappedSegment.get(LONG_HANDLE, offset + 16);
        long payloadLenLong = (long) mappedSegment.get(LONG_HANDLE, offset + 24);
        int payloadLen = checkedCast(payloadLenLong);

        if (offset + HEADER_SIZE + payloadLen > writeOffset) {
            throw new IOException("Corrupted log entry: payload length exceeds write offset at offset " + offset);
        }

        byte[] payload = new byte[payloadLen];
        MemorySegment payloadSlice = mappedSegment.asSlice(offset + HEADER_SIZE, payloadLen);
        MemorySegment.copy(payloadSlice, 0, MemorySegment.ofArray(payload), 0, payloadLen);

        CRC32C crc32c = new CRC32C();
        crc32c.update(payload, 0, payloadLen);
        int computedChecksum = (int) crc32c.getValue();

        if (storedChecksum != computedChecksum) {
            throw new IOException("CRC32C mismatch at offset " + offset + ". Stored: " + storedChecksum + ", Computed: " + computedChecksum);
        }

        return new LogEntry(term, index, payload);
    }

    private void recoverAndValidate() throws IOException {
        long scanOffset = 0;
        long validLimit = 0;

        while (scanOffset + HEADER_SIZE <= SEGMENT_SIZE) {
            int magic = (int) mappedSegment.get(INT_HANDLE, scanOffset);
            if (magic == 0) {
                // Unwritten region reached
                break;
            }
            if (magic != MAGIC_NUMBER) {
                // Corruption encountered, truncate here
                break;
            }

            int storedChecksum = (int) mappedSegment.get(INT_HANDLE, scanOffset + 4);
            long term = (long) mappedSegment.get(LONG_HANDLE, scanOffset + 8);
            long index = (long) mappedSegment.get(LONG_HANDLE, scanOffset + 16);
            long payloadLenLong = (long) mappedSegment.get(LONG_HANDLE, scanOffset + 24);

            if (payloadLenLong < 0 || payloadLenLong > SEGMENT_SIZE) {
                break;
            }
            int payloadLen = (int) payloadLenLong;

            long totalEntrySize = HEADER_SIZE + payloadLen;
            if (scanOffset + totalEntrySize > SEGMENT_SIZE) {
                break;
            }

            // Verify checksum
            byte[] payload = new byte[payloadLen];
            MemorySegment payloadSlice = mappedSegment.asSlice(scanOffset + HEADER_SIZE, payloadLen);
            MemorySegment.copy(payloadSlice, 0, MemorySegment.ofArray(payload), 0, payloadLen);

            CRC32C crc32c = new CRC32C();
            crc32c.update(payload, 0, payloadLen);
            int computedChecksum = (int) crc32c.getValue();

            if (storedChecksum != computedChecksum) {
                // Checksum failure, stop scanning and truncate
                break;
            }

            scanOffset += totalEntrySize;
            validLimit = scanOffset;
        }

        this.writeOffset = validLimit;
    }

    public synchronized void flush() {
        if (!closed) {
            mappedSegment.force();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        flush();
        arena.close();
        channel.close();
        raf.close();
        closed = true;
    }

    public long getBaseIndex() {
        return baseIndex;
    }

    public synchronized long getWriteOffset() {
        return writeOffset;
    }

    public File getFile() {
        return file;
    }

    public synchronized boolean isFull(int incomingEntrySize) {
        return writeOffset + incomingEntrySize > SEGMENT_SIZE;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("WALSegment is closed");
        }
    }

    private static int checkedCast(long value) {
        if (value < 0 || value > Integer.MAX_VALUE) {
            throw new ArithmeticException("Integer overflow during cast: " + value);
        }
        return (int) value;
    }

    public static final class LogEntry {
        private final long term;
        private final long index;
        private final byte[] payload;

        public LogEntry(long term, long index, byte[] payload) {
            this.term = term;
            this.index = index;
            this.payload = payload;
        }

        public long getTerm() {
            return term;
        }

        public long getIndex() {
            return index;
        }

        public byte[] getPayload() {
            return payload;
        }
    }
}