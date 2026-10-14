package com.titan.raft.engine;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.concurrent.Execution;
import org.junit.jupiter.api.concurrent.ExecutionMode;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.logging.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Exhaustive unit, invariant, and concurrent stress-test suite for 'titan-raft-engine'.
 * Designed as a self-contained, high-density test framework validating correctness, 
 * durability, linearizability, and thread-safety under heavy contention and simulated faults.
 */
@Execution(ExecutionMode.CONCURRENT)
public class TitanRaftEngineTestSuite {

    private static final Logger LOGGER = Logger.getLogger(TitanRaftEngineTestSuite.class.getName());
    private Path tempDir;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("titan-raft-test-");
    }

    @AfterEach
    void tearDown() throws IOException {
        if (tempDir != null && Files.exists(tempDir)) {
            Files.walkFileTree(tempDir, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.delete(file);
                    return FileVisitResult.CONTINUE;
                }
                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return FileVisitResult.CONTINUE;
                }
            });
        }
    }

    // =========================================================================
    // 1. UNIT & BOUNDARY TESTS
    // =========================================================================

    @Test
    @DisplayName("Test RaftNode State Machine Initialization and Term Bounds")
    void testNodeInitializationAndTerms() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1", "node-2", "node-3"), tempDir);
        try (RaftNode node = new RaftNode(config)) {
            assertEquals(RaftRole.FOLLOWER, node.getRole());
            assertEquals(0, node.getCurrentTerm());
            assertNull(node.getVotedFor());
            assertEquals(0, node.getLastLogIndex());
            assertEquals(0, node.getLastLogTerm());
        }
    }

    @ParameterizedTest
    @ValueSource(longs = {-1, 0, 100})
    @DisplayName("Test Log Index Boundary Conditions")
    void testLogIndexBoundaries(long initialTerm) throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1"), tempDir);
        try (RaftNode node = new RaftNode(config)) {
            if (initialTerm < 0) {
                assertThrows(IllegalArgumentException.class, () -> node.appendCommand(initialTerm, new byte[0]));
            } else {
                long idx = node.appendCommand(Math.max(1, initialTerm), "test".getBytes());
                assertTrue(idx > 0);
                LogEntry entry = node.getLogEntry(idx);
                assertNotNull(entry);
                assertEquals("test", new String(entry.getData()));
            }
        }
    }

    @Test
    @DisplayName("Test Edge Case: Empty Log Retrieval and Out-of-Bounds Queries")
    void testLogOutOfBounds() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1"), tempDir);
        try (RaftNode node = new RaftNode(config)) {
            assertNull(node.getLogEntry(0));
            assertNull(node.getLogEntry(999));
            assertThrows(NoSuchElementException.class, () -> node.getLogEntryOrThrow(999));
        }
    }

    @Test
    @DisplayName("Test Configuration Validation Bounds")
    void testConfigurationValidation() {
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig("", Set.of("n1"), tempDir));
        assertThrows(IllegalArgumentException.class, () -> new RaftConfig("n1", Collections.emptySet(), tempDir));
        assertThrows(NullPointerException.class, () -> new RaftConfig("n1", null, tempDir));
        assertThrows(NullPointerException.class, () -> new RaftConfig("n1", Set.of("n1"), null));
    }

    // =========================================================================
    // 2. INVARIANT & STATE MACHINE VERIFICATION
    // =========================================================================

    @Test
    @DisplayName("Invariant: Term Monotonicity and Election Safety")
    void testTermMonotonicityAndElectionSafety() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1", "node-2"), tempDir);
        try (RaftNode node = new RaftNode(config)) {
            long initialTerm = node.getCurrentTerm();
            node.handleRequestVote(new RequestVoteArgs(initialTerm + 1, "node-2", 0, 0));
            assertTrue(node.getCurrentTerm() > initialTerm);
            assertEquals("node-2", node.getVotedFor());

            // Invariant: Cannot vote twice in the same term for a different candidate
            boolean secondVote = node.handleRequestVote(new RequestVoteArgs(node.getCurrentTerm(), "node-3", 0, 0));
            assertFalse(secondVote);
            assertEquals("node-2", node.getVotedFor());
        }
    }

    @Test
    @DisplayName("Invariant: Log Matching Property via AppendEntries")
    void testLogMatchingProperty() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1", "node-2"), tempDir);
        try (RaftNode node = new RaftNode(config)) {
            // Append entry on leader/peer
            AppendEntriesArgs validAppend = new AppendEntriesArgs(
                1, "node-2", 0, 0,
                List.of(new LogEntry(1, 1, "cmd1".getBytes())), 0
            );
            AppendEntriesResult result = node.handleAppendEntries(validAppend);
            assertTrue(result.isSuccess());
            assertEquals(1, node.getLastLogIndex());

            // Append with mismatched prevLogIndex
            AppendEntriesArgs invalidAppend = new AppendEntriesArgs(
                1, "node-2", 99, 1,
                List.of(new LogEntry(1, 2, "cmd2".getBytes())), 0
            );
            AppendEntriesResult failResult = node.handleAppendEntries(invalidAppend);
            assertFalse(failResult.isSuccess());
        }
    }

    // =========================================================================
    // 3. FAILURE, DURABILITY & RECOVERY VERIFICATION
    // =========================================================================

    @Test
    @DisplayName("Test State Durability and Crash Recovery via Disk Persistence")
    void testDurabilityAndRecovery() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1", "node-2"), tempDir);
        long lastIndex;
        long term;

        // Write state and log
        try (RaftNode node = new RaftNode(config)) {
            node.forceState(RaftRole.CANDIDATE, 5, "node-1");
            lastIndex = node.appendCommand(5, "persistent-payload".getBytes());
            term = node.getCurrentTerm();
        }

        // Recover node from same directory
        try (RaftNode recoveredNode = new RaftNode(config)) {
            assertEquals(term, recoveredNode.getCurrentTerm());
            assertEquals("node-1", recoveredNode.getVotedFor());
            assertEquals(lastIndex, recoveredNode.getLastLogIndex());
            LogEntry entry = recoveredNode.getLogEntry(lastIndex);
            assertNotNull(entry);
            assertEquals("persistent-payload", new String(entry.getData()));
        }
    }

    @Test
    @DisplayName("Test Split-Brain Prevention via Quorum Checks")
    void testQuorumIsolationRecovery() throws Exception {
        Set<String> cluster = Set.of("n1", "n2", "n3", "n4", "n5");
        RaftConfig config = new RaftConfig("n1", cluster, tempDir);
        try (RaftNode node = new RaftNode(config)) {
            // Force leader promotion
            node.forceState(RaftRole.LEADER, 1, "n1");
            assertTrue(node.isLeader());

            // Simulate loss of quorum network partition
            node.simulateNetworkPartition(true);
            assertFalse(node.hasQuorum(Set.of("n1"))); // Only self out of 5 nodes
            assertTrue(node.hasQuorum(Set.of("n1", "n2", "n3")));
        }
    }

    // =========================================================================
    // 4. CONCURRENT STRESS & RACE CONDITION TESTS
    // =========================================================================

    @Test
    @DisplayName("Concurrent Stress Test: High-Throughput Multi-Threaded Append and Read Contention")
    void testConcurrentAppendAndReadContention() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1", "node-2"), tempDir);
        int threadCount = 16;
        int operationsPerThread = 500;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        AtomicInteger successCount = new AtomicInteger(0);

        try (RaftNode node = new RaftNode(config)) {
            node.forceState(RaftRole.LEADER, 1, "node-1");

            for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                executor.submit(() -> {
                    try {
                        for (int j = 0; j < operationsPerThread; j++) {
                            String payload = "data-" + threadId + "-" + j;
                            long index = node.appendCommand(1, payload.getBytes());
                            if (index > 0) {
                                successCount.incrementAndGet();
                            }
                            if (j % 50 == 0) {
                                node.getLastLogIndex();
                                node.getCurrentTerm();
                            }
                        }
                    } catch (Exception e) {
                        LOGGER.log(Level.SEVERE, "Concurrent operation failed", e);
                    } finally {
                        latch.countDown();
                    }
                });
            }

            assertTrue(latch.await(30, TimeUnit.SECONDS));
            executor.shutdownNow();

            assertEquals(threadCount * operationsPerThread, successCount.get());
            assertEquals(threadCount * operationsPerThread, node.getLastLogIndex());
        }
    }

    @Test
    @DisplayName("Concurrent Stress Test: Interleaved RPCs, State Changes, and Log Appends")
    void testInterleavedRpcsAndMutations() throws Exception {
        RaftConfig config = new RaftConfig("node-1", Set.of("node-1", "node-2", "node-3"), tempDir);
        int workers = 8;
        ExecutorService executor = Executors.newFixedThreadPool(workers);
        AtomicBoolean running = new AtomicBoolean(true);
        CountDownLatch latch = new CountDownLatch(workers);
        Random random = new Random(42);

        try (RaftNode node = new RaftNode(config)) {
            for (int i = 0; i < workers; i++) {
                final int workerId = i;
                executor.submit(() -> {
                    try {
                        while (running.get()) {
                            int action = random.nextInt(3);
                            switch (action) {
                                case 0:
                                    node.appendCommand(1, ("w-" + workerId).getBytes());
                                    break;
                                case 1:
                                    node.handleRequestVote(new RequestVoteArgs(random.nextInt(10) + 1, "node-" + workerId, 0, 0));
                                    break;
                                case 2:
                                    node.handleAppendEntries(new AppendEntriesArgs(
                                        1