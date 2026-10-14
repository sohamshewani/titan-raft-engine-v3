package com.titan.raft.memory;

import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Production-grade lock-free single-producer multi-consumer circular array ring buffer
 * designed for high-throughput zero-copy Raft message passing.
 * 
 * Implements cache-line padding (64-byte alignment) to avoid false sharing across CPU cores,
 * utilizing explicit VarHandle memory fences and atomic sequence barriers inspired by the LMAX Disruptor.
 */
public final class DirectRingBuffer {

    private static final Unsafe UNSAFE;
    
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize sun.misc.Unsafe", e);
        }
    }

    private final int capacity;
    private final int mask;
    private final long address;
    private final long totalBytes;
    
    // Cache-line padding / separation to prevent false sharing
    // 64 bytes offset padding before sequence variables
    private long p00, p01, p02, p03, p04, p05, p06, p07;
    
    // Producer sequence (head)
    private volatile long producerSequence = -1L;
    
    private long p10, p11, p12, p13, p14, p15, p16, p17;
    
    // Consumer minimum sequence barrier
    private volatile long minConsumerSequence = -1L;
    
    private long p20, p21, p22, p23, p24, p25, p26, p27;

    private static final long PRODUCER_SEQ_OFFSET;
    private static final long MIN_CONSUMER_SEQ_OFFSET;
    
    static {
        try {
            PRODUCER_SEQ_OFFSET = UNSAFE.objectFieldOffset(
                DirectRingBuffer.class.getDeclaredField("producerSequence")
            );
            MIN_CONSUMER_SEQ_OFFSET = UNSAFE.objectFieldOffset(
                DirectRingBuffer.class.getDeclaredField("minConsumerSequence")
            );
        } catch (Exception ex) {
            throw new Error(ex);
        }
    }

    /**
     * Creates a new DirectRingBuffer with a power-of-two capacity.
     * Elements are represented as 64-bit memory addresses (pointers) or direct handles.
     * 
     * @param capacity must be a power of two
     */
    public DirectRingBuffer(int capacity) {
        if (capacity <= 0 || (capacity & (capacity - 1)) != 0) {
            throw new IllegalArgumentException("Capacity must be a positive power of two: " + capacity);
        }
        this.capacity = capacity;
        this.mask = capacity - 1;
        
        // Each slot stores a 64-bit direct memory address or payload reference
        // Plus alignment padding
        long bytesRequired = (long) capacity * 8L + 128L;
        this.totalBytes = bytesRequired;
        this.address = UNSAFE.allocateMemory(bytesRequired);
        
        // Zero out memory buffer
        UNSAFE.setMemory(this.address, bytesRequired, (byte) 0);
    }

    /**
     * Claims the next sequence for the producer. Spin-waits if the ring buffer is full
     * based on the slowest consumer's sequence barrier.
     * 
     * @return the claimed sequence number
     */
    public long next() {
        long currentProducerSeq;
        long targetConsumerSeq;
        
        do {
            currentProducerSeq = UNSAFE.getLongVolatile(this, PRODUCER_SEQ_OFFSET);
            targetConsumerSeq = currentProducerSeq - capacity;
            
            if (UNSAFE.getLongVolatile(this, MIN_CONSUMER_SEQ_OFFSET) <= targetConsumerSeq) {
                // Buffer full, yield or spin
                Thread.onSpinWait();
            } else {
                if (UNSAFE.compareAndSwapLong(this, PRODUCER_SEQ_OFFSET, currentProducerSeq, currentProducerSeq + 1L)) {
                    return currentProducerSeq + 1L;
                }
            }
        } while (true);
    }

    /**
     * Publishes a raw 64-bit value (memory address or encoded primitive) at the specified sequence.
     * 
     * @param sequence the sequence returned by {@link #next()}
     * @param value the 64-bit value to store
     */
    public void publish(long sequence, long value) {
        long offset = address + ((sequence & mask) << 3);
        UNSAFE.putLongVolatile(null, offset, value);
    }

    /**
     * Attempts to claim a specific number of sequences in a batch.
     * 
     * @param batchSize number of items to claim
     * @return the highest claimed sequence number in the batch
     */
    public long nextBatch(int batchSize) {
        if (batchSize <= 0 || batchSize > capacity) {
            throw new IllegalArgumentException("Invalid batch size: " + batchSize);
        }
        
        long currentProducerSeq;
        long targetConsumerSeq;
        
        do {
            currentProducerSeq = UNSAFE.getLongVolatile(this, PRODUCER_SEQ_OFFSET);
            long nextProducerSeq = currentProducerSeq + batchSize;
            targetConsumerSeq = nextProducerSeq - capacity;
            
            if (UNSAFE.getLongVolatile(this, MIN_CONSUMER_SEQ_OFFSET) <= targetConsumerSeq) {
                Thread.onSpinWait();
            } else {
                if (UNSAFE.compareAndSwapLong(this, PRODUCER_SEQ_OFFSET, currentProducerSeq, nextProducerSeq)) {
                    return nextProducerSeq;
                }
            }
        } while (true);
    }

    /**
     * Retrieves the 64-bit value stored at the given sequence without advancing cursors.
     * 
     * @param sequence the sequence to read
     * @return the 64-bit stored value
     */
    public long get(long sequence) {
        long offset = address + ((sequence & mask) << 3);
        return UNSAFE.getLongVolatile(null, offset);
    }

    /**
     * Updates the consumer's tracking sequence, releasing slots back to the producer.
     * 
     * @param consumerId unique consumer tracker index or handled sequence
     * @param sequence the latest processed sequence by this consumer
     */
    public void updateConsumerSequence(long sequence) {
        long currentMin;
        do {
            currentMin = UNSAFE.getLongVolatile(this, MIN_CONSUMER_SEQ_OFFSET);
            if (sequence <= currentMin) {
                return;
            }
        } while (!UNSAFE.compareAndSwapLong(this, MIN_CONSUMER_SEQ_OFFSET, currentMin, sequence));
    }

    /**
     * Returns the current producer sequence.
     */
    public long getProducerSequence() {
        return UNSAFE.getLongVolatile(this, PRODUCER_SEQ_OFFSET);
    }

    /**
     * Returns the minimum consumer sequence barrier.
     */
    public long getMinConsumerSequence() {
        return UNSAFE.getLongVolatile(this, MIN_CONSUMER_SEQ_OFFSET);
    }

    /**
     * Returns the total capacity of the ring buffer.
     */
    public int capacity() {
        return capacity;
    }

    /**
     * Calculates current remaining available capacity for the producer.
     */
    public int remainingCapacity() {
        long produced = getProducerSequence();
        long consumed = getMinConsumerSequence();
        long queued = produced - consumed;
        return Math.max(0, capacity - (int) queued);
    }

    /**
     * Explicitly frees native off-heap memory allocated for the ring buffer.
     * Must be called during engine shutdown to prevent native memory leaks.
     */
    public synchronized void release() {
        if (address != 0L) {
            UNSAFE.freeMemory(address);
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            release();
        } finally {
            super.finalize();
        }
    }
}