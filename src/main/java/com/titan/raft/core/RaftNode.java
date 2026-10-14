package com.titan.raft.core;

import sun.misc.Unsafe;

import java.io.File;
import java.io.RandomAccessFile;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32C;

public final class RaftNode implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            java.lang.reflect.Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to acquire Unsafe", e);
        }
    }

    public enum Role {
        FOLLOWER,
        CANDIDATE,
        LEADER
    }

    private static final VarHandle ROLE_HANDLE;
    private static final VarHandle CURRENT_TERM_HANDLE;
    private static final VarHandle VOTED_FOR_HANDLE;
    private static final VarHandle COMMIT_INDEX_HANDLE;
    private static final VarHandle LAST_APPLIED_HANDLE;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            ROLE_HANDLE = l.findVarHandle(RaftNode.class, "role", Role.class);
            CURRENT_TERM_HANDLE = l.findVarHandle(RaftNode.class, "currentTerm", long.class);
            VOTED_FOR_HANDLE = l.findVarHandle(RaftNode.class, "votedFor", long.class);
            COMMIT_INDEX_HANDLE = l.findVarHandle(RaftNode.class, "commitIndex", long.class);
            LAST_APPLIED_HANDLE = l.findVarHandle(RaftNode.class, "lastApplied", long.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize VarHandles", e);
        }
    }

    private volatile Role role = Role.FOLLOWER;
    private volatile long currentTerm = 0L;
    private volatile long votedFor = -1L;
    private volatile long commitIndex = 0L;
    private volatile long lastApplied = 0L;

    private final long nodeId;
    private final File dataDir;
    private final DirectRingBuffer eventQueue;
    private final WALSegment activeWal;
    private final AtomicBoolean running = new AtomicBoolean(true);
    private final Thread eventLoopThread;

    private long lastHeartbeatTimeMs = System.currentTimeMillis();
    private long electionTimeoutMs = randomElectionTimeout();

    public RaftNode(long nodeId, File dataDir) {
        this.nodeId = nodeId;
        this.dataDir = dataDir;
        if (!dataDir.exists() && !dataDir.mkdirs()) {
            throw new IllegalStateException("Failed to create data directory: " + dataDir.getAbsolutePath());
        }

        try {
            File walFile = new File(dataDir, "wal-000001.seg");
            this.activeWal = new WALSegment(walFile, 64L * 1024L * 1024L);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize WALSegment", e);
        }

        this.eventQueue = new DirectRingBuffer(1024);

        this.eventLoopThread = new Thread(this::eventLoop, "RaftEventLoop-" + nodeId);
        this.eventLoopThread.setDaemon(false);
        this.eventLoopThread.start();
    }

    private long randomElectionTimeout() {
        return ThreadLocalRandom.current().nextLong(150, 300);
    }

    public void handleClientRequest(byte[] payload) {
        if ((Role) ROLE_HANDLE.getAcquire(this) != Role.LEADER) {
            throw new IllegalStateException("Not the leader");
        }
        long term = (long) CURRENT_TERM_HANDLE.getAcquire(this);
        long index = activeWal.append(term, payload);
        
        long nextSeq = eventQueue.claimWriterSequence();
        eventQueue.writeEvent(nextSeq, EventType.CLIENT_PROPOSAL, nodeId, term, index, payload);
        eventQueue.publishWriterSequence(nextSeq);
    }

    public Message handleRpc(Message req) {
        long term = (long) CURRENT_TERM_HANDLE.getAcquire(this);
        if (req.term > term) {
            CURRENT_TERM_HANDLE.setRelease(this, req.term);
            ROLE_HANDLE.setRelease(this, Role.FOLLOWER);
            VOTED_FOR_HANDLE.setRelease(this, -1L);
        }

        switch (req.type) {
            case MSG_PRE_VOTE:
                return handlePreVote(req);
            case MSG_VOTE:
                return handleVote(req);
            case MSG_APPEND:
                return handleAppendEntries(req);
            default:
                return new Message(MessageType.MSG_RESPONSE, nodeId, currentTerm, false);
        }
    }

    private Message handlePreVote(Message req) {
        long currentT = (long) CURRENT_TERM_HANDLE.getAcquire(this);
        boolean granted = req.term >= currentT && req.lastLogIndex >= activeWal.getLastIndex();
        return new Message(MessageType.MSG_PRE_VOTE_RESP, nodeId, currentT, granted);
    }

    private Message handleVote(Message req) {
        long currentT = (long) CURRENT_TERM_HANDLE.getAcquire(this);
        long vFor = (long) VOTED_FOR_HANDLE.getAcquire(this);

        if (req.term < currentT) {
            return new Message(MessageType.MSG_VOTE_RESP, nodeId, currentT, false);
        }

        if ((vFor == -1L || vFor == req.senderId) && req.lastLogIndex >= activeWal.getLastIndex()) {
            VOTED_FOR_HANDLE.setRelease(this, req.senderId);
            lastHeartbeatTimeMs = System.currentTimeMillis();
            return new Message(MessageType.MSG_VOTE_RESP, nodeId, currentT, true);
        }

        return new Message(MessageType.MSG_VOTE_RESP, nodeId, currentT, false);
    }

    private Message handleAppendEntries(Message req) {
        long currentT = (long) CURRENT_TERM_HANDLE.getAcquire(this);
        if (req.term < currentT) {
            return new Message(MessageType.MSG_APPEND_RESP, nodeId, currentT, false);
        }

        ROLE_HANDLE.setRelease(this, Role.FOLLOWER);
        CURRENT_TERM_HANDLE.setRelease(this, req.term);
        lastHeartbeatTimeMs = System.currentTimeMillis();

        if (req.payload != null && req.payload.length > 0) {
            activeWal.append(req.term, req.payload);
        }

        if (req.commitIndex > (long) COMMIT_INDEX_HANDLE.getAcquire(this)) {
            COMMIT_INDEX_HANDLE.setRelease(this, Math.min(req.commitIndex, activeWal.getLastIndex()));
        }

        return new Message(MessageType.MSG_APPEND_RESP, nodeId, req.term, true);
    }

    private void eventLoop() {
        long readSeq = eventQueue.getInitialReadSequence();
        while (running.get()) {
            long availableSeq = eventQueue.getAvailableSequence();
            if (readSeq <= availableSeq) {
                DirectRingBuffer.Event event = eventQueue.readEvent(readSeq);
                processEvent(event);
                readSeq++;
            } else {
                checkTimeouts();
                Thread.onSpinWait();
            }
        }
    }

    private void checkTimeouts() {
        long now = System.currentTimeMillis();
        Role currentRole = (Role) ROLE_HANDLE.getAcquire(this);

        if (currentRole != Role.LEADER) {
            if (now - lastHeartbeatTimeMs > electionTimeoutMs) {
                startElection();
            }
        }
    }

    private void startElection() {
        ROLE_HANDLE.setRelease(this, Role.CANDIDATE);
        long newTerm = (long) CURRENT_TERM_HANDLE.getAcquire(this) + 1;
        CURRENT_TERM_HANDLE.setRelease(this, newTerm);
        VOTED_FOR_HANDLE.setRelease(this, nodeId);
        lastHeartbeatTimeMs = System.currentTimeMillis();
        electionTimeoutMs = randomElectionTimeout();

        long nextSeq = eventQueue.claimWriterSequence();
        eventQueue.writeEvent(nextSeq, EventType.START_ELECTION, nodeId, newTerm, activeWal.getLastIndex(), null);
        eventQueue.publishWriterSequence(nextSeq);
    }

    private void processEvent(DirectRingBuffer.Event event) {
        switch (event.type) {
            case START_ELECTION:
                if ((Role) ROLE_HANDLE.getAcquire(this) == Role.CANDIDATE) {
                    ROLE_HANDLE.setRelease(this, Role.LEADER);
                }
                break;
            case CLIENT_PROPOSAL:
                applyCommittedEntries();
                break;
            default:
                break;
        }
    }

    private void applyCommittedEntries() {
        long commit = (long) COMMIT_INDEX_HANDLE.getAcquire(this);
        long applied = (long) LAST_APPLIED_HANDLE.getAcquire(this);
        while (commit > applied) {
            applied++;
            LAST_APPLIED_HANDLE.setRelease(this, applied);
        }
    }

    @Override
    public void close() {
        running.set(false);
        try {
            eventLoopThread.join(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        activeWal.close();
    }

    public static final class WALSegment implements AutoCloseable {
        private final File file;
        private final long capacity;
        private final RandomAccessFile raf;
        private final FileChannel channel;
        private final ByteBuffer mappedBuffer;
        private final long address;
        private long writeOffset = 0L;
        private long lastIndex = 0L;

        public WALSegment(File file, long capacity) throws Exception {
            this.file = file;
            this.capacity = capacity;
            this.raf = new RandomAccessFile(file, "rw");
            this.raf.setLength(capacity);
            this.channel = raf.getChannel();
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, capacity);
            
            java.lang.reflect.Method m = mappedBuffer.getClass().getMethod("address");
            m.setAccessible(true);
            this.address = (long) m.invoke(mappedBuffer);

            recoverAndVerify();
        }

        private void recoverAndVerify() {
            long offset = 0;
            long maxIndex = 0;
            CRC32C crc32c = new CRC32C();

            while (offset + 32 <= capacity) {
                int magic = UNSAFE.getInt(address + offset);
                if (magic != 0x54495441) { // "TITA"
                    break;
                }
                int crc = UNSAFE.getInt(address + offset + 4);
                long term = UNSAFE.getLong(address + offset + 8);
                long index = UNSAFE.getLong(address + offset + 16);
                int payloadLength = UNSAFE.getInt(address + offset + 24);

                long totalEntrySize = 32L + payloadLength;
                if (offset + totalEntrySize > capacity) {
                    break;
                }

                crc32c.reset();
                byte[] payloadBytes = new byte[payloadLength];
                for (int i = 0; i < payloadLength; i++) {
                    payloadBytes[i] = UNSAFE.getByte(address + offset + 32 + i);
                }
                crc32c.update(payloadBytes);
                int computedCrc = (int) crc32c.getValue();

                if (computedCrc != crc) {
                    break;
                }

                maxIndex = Math.max(maxIndex, index);
                offset += totalEntrySize;
            }

            this.writeOffset = offset;
            this.lastIndex = maxIndex;
        }

        public synchronized long append(long term, byte[] payload) {
            int payloadLength = payload.length;
            long totalSize = 32L + payloadLength;
            if (writeOffset + totalSize > capacity) {
                throw new IllegalStateException("WALSegment capacity exceeded");
            }

            CRC32C crc32c = new CRC32C();
            crc32c.update(payload);
            int crc = (int) crc32c.getValue();

            long currentIndex = ++lastIndex;
            long currentAddress = address + writeOffset;

            UNSAFE.putInt(currentAddress, 0x54495441);
            UNSAFE.putInt(currentAddress + 4, crc);
            UNSAFE.putLong(currentAddress + 8, term);
            UNSAFE.putLong(currentAddress + 16, currentIndex);
            UNSAFE.putInt(currentAddress + 24, payloadLength);
            UNSAFE.putInt(currentAddress + 28, 0); // padding

            for (int i = 0; i < payloadLength; i++) {
                UNSAFE.putByte(currentAddress + 32 + i, payload[i]);
            }

            writeOffset += totalSize;
            return currentIndex;
        }

        public long getLastIndex() {
            return lastIndex;
        }

        @Override
        public void close() {
            try {
                channel.close();
                raf.close();
            } catch (Exception e) {
                // suppressed
            }
        }
    }

    public static final class DirectRingBuffer {
        private final int capacity;
        private final int mask;
        private final long[] sequenceBuffer;
        private final Event[] events;

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

        @SuppressWarnings("unused")
        private volatile long head = 0L;
        private volatile long tail = 0L;

        // Cache-line padding simulation via fields
        private long p1, p2, p3, p4, p5, p6, p7;

        public DirectRingBuffer(int capacity) {
            this.capacity = capacity;
            this.mask = capacity - 1;
            this.sequenceBuffer = new long[capacity];
            this.events = new Event[capacity];
            for (int i = 0; i < capacity; i++) {
                events[i] = new Event();
                sequenceBuffer[i] = i;
            }
        }

        public long claimWriterSequence() {
            long currentTail;
            long currentHead;
            do {
                currentTail = (long) TAIL_HANDLE.getAcquire(this);
                currentHead = (long) HEAD_HANDLE.getAcquire(this);
                if (currentTail - currentHead >= capacity) {
                    Thread.onSpinWait();
                }
            } while (!TAIL_HANDLE.compareAndSet(this, currentTail, currentTail + 1));
            return currentTail;
        }

        public void writeEvent(long sequence, EventType type, long senderId, long term, long index, byte[] payload) {
            int idx = (int) (sequence & mask);
            Event e = events[idx];
            e.type = type;
            e.senderId = senderId;
            e.term = term;
            e.index = index;
            e.payload = payload;
            sequenceBuffer[idx] = sequence;
        }

        public void publishWriterSequence(long sequence) {
            int idx = (int) (sequence & mask);
            sequenceBuffer[idx] = sequence + 1;
        }

        public long getInitialReadSequence() {
            return 0L;
        }

        public long getAvailableSequence() {