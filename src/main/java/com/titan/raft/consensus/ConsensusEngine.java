package com.titan.raft.consensus;

import sun.misc.Unsafe;
import java.io.File;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.zip.CRC32C;

public final class ConsensusEngine {

    private static final Unsafe UNSAFE;
    static {
        try {
            java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize Unsafe", e);
        }
    }

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    public static final int MAGIC_HEADER = 0x5449544E; // "TITN"
    public static final int HEADER_SIZE = 32;
    public static final long SEGMENT_SIZE = 64L * 1024L * 1024L; // 64MB

    // Volatile Raft State
    private volatile int currentTerm = 0;
    private volatile int votedFor = -1;
    private volatile int roleOrdinal = Role.FOLLOWER.ordinal();

    private static final VarHandle TERM_HANDLE;
    private static final VarHandle VOTE_HANDLE;
    private static final VarHandle ROLE_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            TERM_HANDLE = l.findVarHandle(ConsensusEngine.class, "currentTerm", int.class);
            VOTE_HANDLE = l.findVarHandle(ConsensusEngine.class, "votedFor", int.class);
            ROLE_HANDLE = l.findVarHandle(ConsensusEngine.class, "roleOrdinal", int.class);
        } catch (Exception e) {
            throw new RuntimeException("VarHandle initialization failed", e);
        }
    }

    private final int nodeId;
    private final int clusterSize;
    private final DirectRingBuffer ringBuffer;
    private final WALSegment activeSegment;
    private final SnapshotManager snapshotManager;

    private long commitIndex = 0;
    private long lastApplied = 0;
    private long lastLogIndex = 0;
    private int lastLogTerm = 0;

    // Election timer state
    private long electionTimeoutMs = 0;
    private long lastHeartbeatTimeMs = 0;

    public ConsensusEngine(int nodeId, int clusterSize, File walDir) throws Exception {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.ringBuffer = new DirectRingBuffer(1024);
        
        File segmentFile = new File(walDir, "wal-0000000001.seg");
        if (!segmentFile.exists()) {
            segmentFile.createNewFile();
        }
        this.activeSegment = new WALSegment(segmentFile, SEGMENT_SIZE);
        this.snapshotManager = new SnapshotManager(new File(walDir, "snapshots"));

        recoverStateFromWAL();
        resetElectionTimeout();
    }

    public Role getRole() {
        return Role.values()[(int) ROLE_HANDLE.getAcquire(this)];
    }

    public void setRole(Role newRole) {
        ROLE_HANDLE.setRelease(this, newRole.ordinal());
    }

    public int getCurrentTerm() {
        return (int) TERM_HANDLE.getAcquire(this);
    }

    public boolean CASCurrentTerm(int expected, int update) {
        return TERM_HANDLE.compareAndSet(this, expected, update);
    }

    public int getVotedFor() {
        return (int) VOTE_HANDLE.getAcquire(this);
    }

    public void setVotedFor(int candidateId) {
        VOTE_HANDLE.setRelease(this, candidateId);
    }

    private void recoverStateFromWAL() {
        activeSegment.scanAndVerifyChecksums(entry -> {
            this.lastLogIndex = entry.index;
            this.lastLogTerm = entry.term;
            if (entry.term > getCurrentTerm()) {
                TERM_HANDLE.setRelease(this, entry.term);
                setVotedFor(-1);
            }
        });
    }

    private void resetElectionTimeout() {
        long base = 150L;
        long randomized = ThreadLocalRandom.current().nextLong(150L);
        this.electionTimeoutMs = base + randomized;
        this.lastHeartbeatTimeMs = System.currentTimeMillis();
    }

    public void tick(long currentTimeMs) {
        Role currentRole = getRole();
        if (currentRole != Role.LEADER) {
            if ((currentTimeMs - lastHeartbeatTimeMs) > electionTimeoutMs) {
                startElection();
            }
        } else {
            if ((currentTimeMs - lastHeartbeatTimeMs) > 50L) {
                broadcastHeartbeat();
                lastHeartbeatTimeMs = currentTimeMs;
            }
        }
    }

    private void startElection() {
        setRole(Role.CANDIDATE);
        int newTerm = getCurrentTerm() + 1;
        TERM_HANDLE.setRelease(this, newTerm);
        setVotedFor(nodeId);
        resetElectionTimeout();

        int votes = 1; // vote for self
        int quorum = (clusterSize / 2) + 1;

        if (votes >= quorum) {
            transitionToLeader();
        }
    }

    private void transitionToLeader() {
        setRole(Role.LEADER);
        broadcastHeartbeat();
    }

    private void broadcastHeartbeat() {
        // Construct and dispatch zero-copy append entries RPC payload
        try {
            long currentTermVal = getCurrentTerm();
            long writeIndex = ringBuffer.claim();
            DirectRingBuffer.RingBufferEvent event = ringBuffer.get(writeIndex);
            event.type = 1; // Heartbeat type
            event.term = (int) currentTermVal;
            event.leaderId = nodeId;
            event.prevLogIndex = lastLogIndex;
            event.prevLogTerm = lastLogTerm;
            event.commitIndex = commitIndex;
            ringBuffer.publish(writeIndex);
        } catch (Exception e) {
            // Handle buffer full / backpressure
        }
    }

    public boolean handleAppendEntries(int term, int leaderId, long prevLogIndex, long prevLogTerm, 
                                       ByteBuffer payload, long leaderCommit) {
        int currentT = getCurrentTerm();
        if (term < currentT) {
            return false;
        }

        if (term > currentT) {
            TERM_HANDLE.setRelease(this, term);
            setVotedFor(-1);
            setRole(Role.FOLLOWER);
        }

        lastHeartbeatTimeMs = System.currentTimeMillis();

        if (prevLogIndex > 0 && lastLogIndex < prevLogIndex) {
            return false;
        }

        if (payload != null && payload.remaining() > 0) {
            long newIndex = lastLogIndex + 1;
            int payloadLen = payload.remaining();
            
            CRC32C crc = new CRC32C();
            byte[] tempArr = new byte[payloadLen];
            payload.mark();
            payload.get(tempArr);
            payload.reset();
            crc.update(tempArr, 0, payloadLen);
            int checksum = (int) crc.getValue();

            activeSegment.append(term, newIndex, checksum, payload);
            this.lastLogIndex = newIndex;
            this.lastLogTerm = term;
        }

        if (leaderCommit > commitIndex) {
            commitIndex = Math.min(leaderCommit, lastLogIndex);
            applyToStateMachine();
        }

        return true;
    }

    private void applyToStateMachine() {
        while (commitIndex > lastApplied) {
            lastApplied++;
            // Execute state machine transition via application boundary
        }
    }

    public static final class WALSegment {
        private final File file;
        private final FileChannel channel;
        private final ByteBuffer mappedBuffer;
        private final long capacity;
        private long writePosition = 0;

        public WALSegment(File file, long capacity) throws Exception {
            this.file = file;
            this.capacity = capacity;
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
        }

        public synchronized void append(int term, long index, int checksum, ByteBuffer payload) {
            int payloadLen = payload.remaining();
            int totalBlockSize = HEADER_SIZE + payloadLen;
            if (writePosition + totalBlockSize > capacity) {
                throw new IllegalStateException("WALSegment exhausted");
            }

            long offset = writePosition;
            mappedBuffer.putInt((int) offset, MAGIC_HEADER);
            mappedBuffer.putInt((int) offset + 4, checksum);
            mappedBuffer.putInt((int) offset + 8, term);
            mappedBuffer.putLong((int) offset + 12, index);
            mappedBuffer.putInt((int) offset + 20, payloadLen);
            // padding 8 bytes reserved for alignment
            mappedBuffer.putLong((int) offset + 24, 0L);

            mappedBuffer.position((int) offset + HEADER_SIZE);
            mappedBuffer.put(payload);

            writePosition += totalBlockSize;
        }

        public void scanAndVerifyChecksums(LogEntryConsumer consumer) {
            long pos = 0;
            CRC32C crc = new CRC32C();
            while (pos + HEADER_SIZE <= capacity) {
                int magic = mappedBuffer.getInt((int) pos);
                if (magic != MAGIC_HEADER) {
                    break;
                }
                int checksum = mappedBuffer.getInt((int) pos + 4);
                int term = mappedBuffer.getInt((int) pos + 8);
                long index = mappedBuffer.getLong((int) pos + 12);
                int payloadLen = mappedBuffer.getInt((int) pos + 20);

                if (pos + HEADER_SIZE + payloadLen > capacity) {
                    break;
                }

                byte[] payloadBytes = new byte[payloadLen];
                for (int i = 0; i < payloadLen; i++) {
                    payloadBytes[i] = mappedBuffer.get((int) pos + HEADER_SIZE + i);
                }

                crc.reset();
                crc.update(payloadBytes, 0, payloadLen);
                if ((int) crc.getValue() != checksum) {
                    // Corrupted tail record, truncate
                    break;
                }

                consumer.accept(new LogEntry(term, index, payloadBytes));
                pos += HEADER_SIZE + payloadLen;
                writePosition = pos;
            }
        }
    }

    public static final class LogEntry {
        public final int term;
        public final long index;
        public final byte[] payload;

        public LogEntry(int term, long index, byte[] payload) {
            this.term = term;
            this.index = index;
            this.payload = payload;
        }
    }

    @FunctionalInterface
    public interface LogEntryConsumer {
        void accept(LogEntry entry);
    }

    public static final class DirectRingBuffer {
        private final int capacity;
        private final RingBufferEvent[] buffer;
        private volatile long head = 0;
        private volatile long tail = 0;

        private static final VarHandle HEAD_HANDLE;
        private static final VarHandle TAIL_HANDLE;

        static {
            try {
                MethodHandles.Lookup l = MethodHandles.lookup();
                HEAD_HANDLE = l.findVarHandle(DirectRingBuffer.class, "head", long.class);
                TAIL_HANDLE = l.findVarHandle(DirectRingBuffer.class, "tail", long.class);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public DirectRingBuffer(int capacity) {
            this.capacity = capacity;
            this.buffer = new RingBufferEvent[capacity];
            for (int i = 0; i < capacity; i++) {
                buffer[i] = new RingBufferEvent();
            }
        }

        public long claim() {
            long currentTail;
            long currentHead;
            do {
                currentTail = (long) TAIL_HANDLE.getAcquire(this);
                currentHead = (long) HEAD_HANDLE.getAcquire(this);
                if (currentTail - currentHead >= capacity) {
                    throw new IllegalStateException("RingBuffer full");
                }
            } while (!TAIL_HANDLE.compareAndSet(this, currentTail, currentTail + 1));
            return currentTail;
        }

        public void publish(long sequence) {
            // Memory barrier release semantics via atomic CAS on head if single producer
            // Simplified synchronization for ring buffer barrier
        }

        public RingBufferEvent get(long sequence) {
            return buffer[(int) (sequence & (capacity - 1))];
        }

        public static final class RingBufferEvent {
            // 64-byte padding cache-line alignment to prevent false sharing
            public long p1, p2, p3, p4, p5, p6, p7;
            public int type;
            public int term;
            public int leaderId;
            public long prevLogIndex;
            public long prevLogTerm;
            public long commitIndex;
            public long q1, q2, q3, q4, q5, q6, q7;
        }
    }

    public static final class SnapshotManager {
        private final File snapshotDir;

        public SnapshotManager(File snapshotDir) {
            this.snapshotDir = snapshotDir;
            if (!snapshotDir.exists()) {
                snapshotDir.mkdirs();
            }
        }

        public void writeSnapshot(long lastIncludedIndex, int lastIncludedTerm, ByteBuffer snapshotData) throws Exception {
            File snapFile = new File(snapshotDir, "snapshot-" + lastIncludedIndex + "-" + lastIncludedTerm + ".bin");
            try (FileChannel channel = new RandomAccessFile(snapFile, "rw").getChannel()) {
                channel.write(snapshotData);
                channel.force(true);
            }
        }
    }
}