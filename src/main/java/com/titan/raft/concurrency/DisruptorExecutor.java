package com.titan.raft.concurrency;

import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorOneArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public final class DisruptorExecutor implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(DisruptorExecutor.class);

    private final int ringBufferSize;
    private final Disruptor<RaftEvent> disruptor;
    private final RingBuffer<RaftEvent> ringBuffer;
    private final AtomicBoolean isRunning;
    private final RaftEventHandler eventHandler;

    public enum EventType {
        APPEND_ENTRIES_REQUEST,
        APPEND_ENTRIES_RESPONSE,
        REQUEST_VOTE_REQUEST,
        REQUEST_VOTE_RESPONSE,
        CLIENT_PROPOSAL,
        HEARTBEAT_TIMEOUT,
        ELECTION_TIMEOUT,
        SNAPSHOT_INSTALL
    }

    public static final class RaftEvent {
        private EventType type;
        private long term;
        private int sourceNodeId;
        private int targetNodeId;
        private ByteBuffer payload;
        private Object context;

        public EventType getType() {
            return type;
        }

        public void setType(EventType type) {
            this.type = type;
        }

        public long getTerm() {
            return term;
        }

        public void setTerm(long term) {
            this.term = term;
        }

        public int getSourceNodeId() {
            return sourceNodeId;
        }

        public void setSourceNodeId(int sourceNodeId) {
            this.sourceNodeId = sourceNodeId;
        }

        public int getTargetNodeId() {
            return targetNodeId;
        }

        public void setTargetNodeId(int targetNodeId) {
            this.targetNodeId = targetNodeId;
        }

        public ByteBuffer getPayload() {
            return payload;
        }

        public void setPayload(ByteBuffer payload) {
            this.payload = payload;
        }

        public Object getContext() {
            return context;
        }

        public void setContext(Object context) {
            this.context = context;
        }

        public void clear() {
            this.type = null;
            this.term = 0L;
            this.sourceNodeId = -1;
            this.targetNodeId = -1;
            this.payload = null;
            this.context = null;
        }
    }

    public interface RaftEventListener {
        void onEvent(RaftEvent event) throws Exception;
    }

    private static final class RaftEventFactory implements EventFactory<RaftEvent> {
        @Override
        public RaftEvent newInstance() {
            return new RaftEvent();
        }
    }

    private static final class RaftEventHandler implements EventHandler<RaftEvent> {
        private final RaftEventListener listener;

        private RaftEventHandler(RaftEventListener listener) {
            this.listener = Objects.requireNonNull(listener, "listener cannot be null");
        }

        @Override
        public void onEvent(RaftEvent event, long sequence, boolean endOfBatch) throws Exception {
            try {
                listener.onEvent(event);
            } catch (Throwable t) {
                logger.error("Uncaught exception in Disruptor event loop at sequence={}", sequence, t);
                throw t;
            } finally {
                event.clear();
            }
        }
    }

    private static final class RaftThreadFactory implements ThreadFactory {
        private final String threadName;

        private RaftThreadFactory(String threadName) {
            this.threadName = Objects.requireNonNull(threadName, "threadName cannot be null");
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, threadName);
            t.setDaemon(true);
            t.setUncaughtExceptionHandler((thread, ex) -> 
                logger.error("Uncaught exception in thread {}", thread.getName(), ex)
            );
            return t;
        }
    }

    private static final EventTranslatorOneArg<RaftEvent, EventPayloadHolder> TRANSLATOR = 
        (event, sequence, holder) -> {
            event.setType(holder.type);
            event.setTerm(holder.term);
            event.setSourceNodeId(holder.sourceNodeId);
            event.setTargetNodeId(holder.targetNodeId);
            event.setPayload(holder.payload);
            event.setContext(holder.context);
        };

    private static final class EventPayloadHolder {
        private final EventType type;
        private final long term;
        private final int sourceNodeId;
        private final int targetNodeId;
        private final ByteBuffer payload;
        private final Object context;

        private EventPayloadHolder(EventType type, long term, int sourceNodeId, int targetNodeId, ByteBuffer payload, Object context) {
            this.type = type;
            this.term = term;
            this.sourceNodeId = sourceNodeId;
            this.targetNodeId = targetNodeId;
            this.payload = payload;
            this.context = context;
        }
    }

    public DisruptorExecutor(int ringBufferSize, String threadName, RaftEventListener listener) {
        if (Integer.bitCount(ringBufferSize) != 1) {
            throw new IllegalArgumentException("ringBufferSize must be a power of 2, got: " + ringBufferSize);
        }
        this.ringBufferSize = ringBufferSize;
        this.isRunning = new AtomicBoolean(false);

        this.disruptor = new Disruptor<>(
            new RaftEventFactory(),
            this.ringBufferSize,
            new RaftThreadFactory(threadName),
            ProducerType.MULTI,
            new SleepingWaitStrategy()
        );

        this.eventHandler = new RaftEventHandler(listener);
        this.disruptor.handleEventsWith(this.eventHandler);
    }

    public void start() {
        if (isRunning.compareAndSet(false, true)) {
            logger.info("Starting DisruptorExecutor with ring buffer size={}", ringBufferSize);
            disruptor.start();
            this.ringBuffer = disruptor.getRingBuffer();
        } else {
            logger.warn("DisruptorExecutor is already running.");
        }
    }

    public void publish(EventType type, long term, int sourceNodeId, int targetNodeId, ByteBuffer payload, Object context) {
        ensureRunning();
        EventPayloadHolder holder = new EventPayloadHolder(type, term, sourceNodeId, targetNodeId, payload, context);
        ringBuffer.publishEvent(TRANSLATOR, holder);
    }

    public void publishAsync(EventType type, long term, int sourceNodeId, int targetNodeId, ByteBuffer payload, Object context) {
        publish(type, term, sourceNodeId, targetNodeId, payload, context);
    }

    public boolean tryPublish(EventType type, long term, int sourceNodeId, int targetNodeId, ByteBuffer payload, Object context) {
        ensureRunning();
        RingBuffer<RaftEvent> rb = this.ringBuffer;
        if (rb.hasAvailableCapacity(1)) {
            EventPayloadHolder holder = new EventPayloadHolder(type, term, sourceNodeId, targetNodeId, payload, context);
            return rb.tryPublishEvent(TRANSLATOR, holder);
        }
        return false;
    }

    public long getCursor() {
        ensureRunning();
        return ringBuffer.getCursor();
    }

    public long getRemainingCapacity() {
        ensureRunning();
        return ringBuffer.remainingCapacity();
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    private void ensureRunning() {
        if (!isRunning.get()) {
            throw new IllegalStateException("DisruptorExecutor is not running.");
        }
    }

    @Override
    public void close() {
        if (isRunning.compareAndSet(true, false)) {
            logger.info("Shutting down DisruptorExecutor...");
            try {
                disruptor.shutdown(10, TimeUnit.SECONDS);
                logger.info("DisruptorExecutor successfully shut down.");
            } catch (Exception e) {
                logger.error("Error during disruptor shutdown, forcing halt", e);
                try {
                    disruptor.halt();
                } catch (Exception inner) {
                    logger.error("Failed to halt disruptor cleanly", inner);
                }
            }
        }
    }
}