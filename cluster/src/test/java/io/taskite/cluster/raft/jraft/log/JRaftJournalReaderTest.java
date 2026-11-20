/*
 * Copyright (c) 2026 lzgabel and Taskite contributors
 *
 * Licensed under the Taskite Community License, Version 1.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at:
 *
 *     https://github.com/taskite-io/taskite/blob/main/LICENSE
 *
 * The Software is source-available for non-commercial use and limited evaluation
 * use. Other commercial use requires a separate written license. The Software is
 * provided "AS IS", without warranties or conditions of any kind.
 */
package io.taskite.cluster.raft.jraft.log;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alipay.sofa.jraft.Node;
import com.alipay.sofa.jraft.entity.UserLog;
import com.alipay.sofa.jraft.error.LogNotFoundException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

final class JRaftJournalReaderTest {
  @Test
  void executionAuthorityIsSkippedBeforeBusinessRecords() {
    final Node node =
        (Node)
            Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getLastAppliedLogIndex")) {
                    return 2L;
                  }
                  if (method.getName().equals("readCommittedUserLog")) {
                    return (long) args[0] == 1
                        ? new UserLog(1, JRaftLogEntryCodec.executionAuthority(1, "successor"))
                        : new UserLog(2, JRaftLogEntryCodec.encode(1, 1, new byte[] {42}));
                  }
                  throw new UnsupportedOperationException(method.getName());
                });
    final var reader = new JRaftJournalReader(node);
    assertTrue(reader.hasNext());
    assertEquals(2, reader.next().index());
    assertFalse(reader.hasNext());
  }

  @Test
  void skipsLargeCompactedPrefixWithoutRecursion() {
    final long firstAvailableIndex = 100_000;
    final byte[] payload = "available".getBytes(StandardCharsets.UTF_8);
    final AtomicInteger probes = new AtomicInteger();
    final Node node =
        (Node)
            Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getLastAppliedLogIndex")) {
                    return firstAvailableIndex;
                  }
                  if (method.getName().equals("readCommittedUserLog")) {
                    probes.incrementAndGet();
                    final long index = (long) args[0];
                    if (index < firstAvailableIndex) {
                      throw new LogNotFoundException("compacted index " + index);
                    }
                    return new UserLog(
                        firstAvailableIndex, JRaftLogEntryCodec.encode(7, 7, payload));
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    final JRaftJournalReader reader = new JRaftJournalReader(node);

    assertTrue(reader.hasNext());
    final var record = reader.next();
    assertEquals(firstAvailableIndex, record.index());
    assertEquals(7, record.lowestPosition());
    assertEquals(7, record.highestPosition());
    assertArrayEquals(payload, record.data());
    assertEquals(firstAvailableIndex + 1, reader.nextIndex());
    assertTrue(probes.get() < 32, "compacted prefix lookup must be logarithmic");
  }

  @Test
  void advancesPastAppliedTailWhenOnlyInternalEntriesRemainAfterCompaction() {
    final long firstAvailableIndex = 50_000;
    final long lastAppliedIndex = 100_000;
    final AtomicInteger probes = new AtomicInteger();
    final Node node =
        (Node)
            Proxy.newProxyInstance(
                Node.class.getClassLoader(),
                new Class<?>[] {Node.class},
                (proxy, method, args) -> {
                  if (method.getName().equals("getLastAppliedLogIndex")) {
                    return lastAppliedIndex;
                  }
                  if (method.getName().equals("readCommittedUserLog")) {
                    probes.incrementAndGet();
                    final long index = (long) args[0];
                    if (index < firstAvailableIndex) {
                      throw new LogNotFoundException("compacted index " + index);
                    }
                    throw new IllegalStateException("only internal entries remain");
                  }
                  throw new UnsupportedOperationException(method.getName());
                });

    final JRaftJournalReader reader = new JRaftJournalReader(node);

    assertFalse(reader.hasNext());
    assertEquals(lastAppliedIndex + 1, reader.nextIndex());
    assertTrue(probes.get() < 32, "compacted prefix lookup must be logarithmic");
  }
}
